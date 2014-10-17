#! /bin/env python
# -*- coding: utf-8 -*-

'''
Download SQL file of MySQL database by phpMyAdmin
'''

import re
import os
import sys
import base64
import urllib
import urllib2
import traceback
from cookielib import CookieJar, DefaultCookiePolicy
from pprint import pprint

__author__    = 'furyu (furyutei@gmail.com)'
__version__   = '0.0.1d'
__copyright__ = 'Copyright (c) 2014 furyu'
__license__   = 'New BSD License'


class LoadSqlDump(object): #{
  #{ class variables
  DEFAULT_HEADER_DICT = {
    'Accept-Charset': 'Shift_JIS,utf-8;q=0.7,*;q=0.7',
    'Accept-Language': 'ja,en-us;q=0.7,en;q=0.3',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36',
  }
  DEFAULT_PMA_LOGIN_PARAM_DICT = dict(
    server = u'1',
    target = u'index.php',
  )
  DEFAULT_PARAM_DICT = dict(
    server = u'1',
    export_type = u'server',
    export_method = u'quick',
    quick_or_custom = u'custom',
    output_format = u'sendit',
    filename_template = u'@SERVER@',
    remember_template = u'on',
    charset_of_file = u'utf-8',
    compression = u'none', # none, zip, gzip
    what = u'sql',
    codegen_structure_or_data = u'data',
    codegen_format = u'0',
    csv_separator = u'',
    csv_enclosed = u'"',
    csv_escaped = u'"',
    csv_terminated = u'AUTO',
    csv_null = u'NULL',
    csv_structure_or_data = u'data',
    excel_null = u'NULL',
    excel_edition = u'win',
    excel_structure_or_data = u'data',
    htmlword_structure_or_data = u'structure_and_data',
    htmlword_null = u'NULL',
    json_structure_or_data = u'data',
    latex_caption = u'something',
    latex_structure_or_data = u'structure_and_data',
    latex_structure_caption = u'テーブル @TABLE@ の構造',
    latex_structure_continued_caption = u'テーブル @TABLE@ の構造 (続き)',
    latex_structure_label = u'tab:@TABLE@-structure',
    latex_comments = u'something',
    latex_columns = u'something',
    latex_data_caption = u'テーブル @TABLE@ の内容',
    latex_data_continued_caption = u'テーブル @TABLE@ の内容 (続き)',
    latex_data_label = u'tab:@TABLE@-data',
    latex_null = u'\textit{NULL}',
    mediawiki_structure_or_data = u'data',
    ods_null = u'NULL',
    ods_structure_or_data = u'data',
    odt_structure_or_data = u'structure_and_data',
    odt_comments = u'something',
    odt_columns = u'something',
    odt_null = u'NULL',
    pdf_report_title = u'',
    pdf_structure_or_data = u'data',
    php_array_structure_or_data = u'data',
    sql_include_comments = u'something',
    sql_header_comment = u'',
    sql_compatibility = u'NONE',
    sql_structure_or_data = u'structure_and_data',
    sql_drop_table = u'something', # "DROP TABLE / VIEW / PROCEDURE / FUNCTION コマンドの追加" にチェック
    sql_procedure_function = u'something',
    sql_create_table_statements = u'something',
    sql_if_not_exists = u'something',
    sql_auto_increment = u'something',
    sql_backquotes = u'something',
    sql_type = u'INSERT',
    sql_insert_syntax = u'both',
    sql_max_query_size = u'50000',
    sql_hex_for_blob = u'something',
    sql_utc_time = u'something',
    texytext_structure_or_data = u'structure_and_data',
    texytext_null = u'NULL',
    yaml_structure_or_data = u'data',
    knjenc = u'',
  )
  RE_TOKEN = re.compile(u"token\s*=[^\w]*(\w+)[^\w]")
  
  KB = 1024
  MB = 1024*1024
  BUFSIZE = 256*1024
  
  CODEC_LIST = ('utf_8', 'euc_jp', 'cp932',) # see http://docs.python.org/2.7/library/codecs.html#standard-encodings
  RE_ESC_SEQ = re.compile('\x1b($B|$@|\(B|\(J| A)')
  
  PMA_CODEC = 'utf-8'
  #} // end of class variables
  
  def __init__(self, url_phpmyadmin_top, user=None, passwd=None, pma_username=None, pma_password=None, tgt_dir=None, server_number=1, quiet=False, param_dict=None): #{
    '''
      url_phpmyadmin_top: URL of phpMyAdmin's toppage
      user              : user name for Basic Authentication
      passwd            : password for Basic Authentication
      pma_username      : user name for phpMyAdmin
      pma_password      : password for phpMyAdmin
      tgt_dir           : directory to save
      server_number     : MySQL server number
      quiet             : (True) quiet mode
      param_dict        : additional parameter's dictionary to export
    '''
    (src_codec, url_phpmyadmin_top) = self._str_decode(url_phpmyadmin_top)
    url_phpmyadmin_top = re.sub(u'/index\.php(\?.*)?$', ur'', url_phpmyadmin_top)
    if not re.search(u'/$', url_phpmyadmin_top): url_phpmyadmin_top += '/'
    self.url_phpmyadmin_top = url_phpmyadmin_top
    self.pma_username = pma_username
    self.pma_password = pma_password
    self.quiet = quiet
    self.last_url = ''
    try:
      self.server_number = unicode(int(server_number))
    except:
      self.server_number = u'1'
    
    self.header_dict = self.DEFAULT_HEADER_DICT.copy()
    if user and passwd:
      self.header_dict['Authorization'] = 'Basic %s' % (base64.b64encode('%s:%s' % (user, passwd)))
    
    self.login_param_dict = self.DEFAULT_PMA_LOGIN_PARAM_DICT.copy()
    self.login_param_dict['server'] = self.server_number
    
    self.param_dict = self.DEFAULT_PARAM_DICT.copy()
    if isinstance(param_dict, dict): self.param_dict.update(param_dict)
    self.param_dict['server'] = self.server_number
    
    self.url_opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(CookieJar(policy=DefaultCookiePolicy(rfc2965=True, netscape=True))))
    
    self.filename_codec = sys.getfilesystemencoding()
    
    if tgt_dir:
      (src_codec, tgt_dir) = self._str_decode(tgt_dir)
    else:
      tgt_dir = '.'
    
    tgt_dir_enc = tgt_dir.encode(self.filename_codec, 'ignore')
    if not os.path.isdir(tgt_dir_enc):
      try:
        os.makedirs(tgt_dir_enc)
      except:
        print >> sys.stderr, traceback.format_exc()
        print >> sys.stderr, u'Error: cannot create "%s"' % (tgt_dir)
        tgt_dir = '.'
        tgt_dir_enc = tgt_dir.encode(self.filename_codec, 'ignore')
    
    self.tgt_dir = tgt_dir
    self.tgt_dir_enc = tgt_dir_enc
    if not self.quiet:
      print u'phpMyAdmin: %s' % (self.url_phpmyadmin_top)
      print u'directory : %s' % (tgt_dir)
    
  #} // end of def __init__()
  
  def do(self, db_name): #{
    """
      download "<db_name>.sql" via phpMyAdmin
    """
    (src_codec, db_name) = self._str_decode(db_name)
    filename = u'%s.sql' % (db_name)
    if not self.quiet:
      print u'%s' % (filename)
    
    flg_success = False
    while True:
      token = self._get_token()
      if not token: break
      
      url = self.url_phpmyadmin_top + 'export.php'
      self.param_dict.update(
        token = token,
        db_select = [db_name],
      )
      rsp = self._fetch(url, data=self._make_data(self.param_dict))
      if rsp.code < 200 or 300 <= rsp.code:
        print >> sys.stderr, u'Error: %s => %d: %s' % (url, rsp.code, rsp.msg)
        break
      
      filename_enc = os.path.join(self.tgt_dir_enc, filename.encode(self.filename_codec, 'ignore'))
      fp = open(filename_enc, 'wb')
      #info = rsp.info()
      #for key in info.keys():
      #  print '%s=%s' % (key, info.getheader(key))
      
      #fp.write(rsp.read())
      size = 0
      for buf in iter(lambda:rsp.read(self.BUFSIZE),''):
        fp.write(buf)
        size += len(buf)
        if not self.quiet:
          if size < self.MB:
            print '\r%6d KB' % (size//self.KB),
          else:
            print '\r%6.2f MB' % (float(size)/self.MB),
          sys.stdout.flush()
      fp.close()
      if not self.quiet: print ''
      
      flg_success = True
      break
    return flg_success
  #} // end of def exec()
  
  def _get_token(self): #{
    def _get_token_from_rsp(rsp): #{
      (token, content) = (None, None)
      while True:
        if rsp.code < 200 or 300 <= rsp.code:
          print >> sys.stderr, u'Error: %s => %d: %s' % (url, rsp.code, rsp.msg)
          break
        
        (src_codec, content) = self._str_decode(rsp.read())
        mrslt = self.RE_TOKEN.search(content)
        if not mrslt:
          print >> sys.stderr, u'Error: token not found'
          break
        
        token = mrslt.group(1)
        break
      
      return (token, content)
    #} // end of def _get_token_from_rsp()
    
    url = self.url_phpmyadmin_top
    rsp = self._fetch(url)
    (token, content) = _get_token_from_rsp(rsp)
    while token:
      if not re.search(u'name="pma_username"', content): break
      (pma_username, pma_password) = (self.pma_username, self.pma_password)
      if not pma_username or not pma_password:
        print >> sys.stderr, u'Error: both pma_username and pma_password required'
        token = None
        break
      
      param_dict = self.login_param_dict
      param_dict.update(dict(
        pma_username = pma_username,
        pma_password = pma_password,
        token = token,
      ))
      rsp = self._fetch(url, data=self._make_data(param_dict))
      (token, content) = _get_token_from_rsp(rsp)
      if re.search(u'name="pma_username"', content):
        print >> sys.stderr, u'Error: incorrect pma_username or pma_password'
        token = None
        break
      break
    
    self.token = token
    
    return token
  #} // end of def _get_token()
  
  def _quote(self, param, charset='utf-8'): #{
    return urllib.quote(param.encode(charset, 'ignore'), safe='~')
  #} // end of def _quote()
  
  def _make_data(self, param_dict): #{
    query_list=[]
    quote = lambda s: self._quote(s, self.PMA_CODEC)
    for key in sorted(param_dict.keys()):
      if isinstance(param_dict[key], list):
        for param in param_dict[key]:
          query_list.append('%s[]=%s' % (quote(key), quote(param)))
      else:
        query_list.append('%s=%s' % (quote(key), quote(param_dict[key])))
    return '&'.join(query_list)
  #} // end of def _make_data()
  
  def _fetch(self, url, data=None, headers=None): #{
    mrslt = re.search(u'^(https?://)([^/]+)(.*)$', url)
    proto = mrslt.group(1).encode(self.PMA_CODEC,'ignore')
    domain = mrslt.group(2).encode('idna')
    path = mrslt.group(3).encode(self.PMA_CODEC, 'ignore')
    url = proto + domain + path
    if not headers:
      headers = self.header_dict
      headers['Referer'] = self.last_url
    rsp = self.url_opener.open(urllib2.Request(url, data=data, headers=headers))
    self.last_url = rsp.geturl()
    return rsp
  #} // end of def _fetch()
  
  def _str_decode(self, src_str): #{
    (src_codec, dec_str) = (None, src_str)
    while True:
      if not isinstance(src_str, basestring): break
      if isinstance(src_str, unicode):
        src_codec = 'unicode_internal'
        break
      try:
        dec_str = src_str.decode('iso2022_jp')
        src_codec = self.RE_ESC_SEQ.search(src_str) and 'iso2022_jp' or 'ascii'
        break
      except UnicodeDecodeError, s:
        pass
        
      for test_codec in self.CODEC_LIST:
        try:
          dec_str = src_str.decode(test_codec)
          src_codec = test_codec
          break
        except UnicodeDecodeError, s:
          pass
      break
    return (src_codec, dec_str)
  #} // end of def _str_decode()
  
#} // end of class LoadSqlDump()


if __name__ == '__main__': #{
  import optparse
  
  usage = u"./%prog [options] <phpMyAdmin's URL> <database name> [<database name> ...]"
  optparser = optparse.OptionParser(usage=usage, version=__version__)

  optparser.add_option(
    '-u', '--ba-user',
    action = 'store',
    metavar = '<BASIC-AUTH USER>',
    help = u"user name for Basic Authentication",
    dest = 'user'
  )
  
  optparser.add_option(
    '-p', '--ba-passwd',
    action = 'store',
    metavar = '<BASIC-AUTH PASSWORD>',
    help = u"password for Basic Authentication",
    dest = 'passwd'
  )

  optparser.add_option(
    '-n', '--pma-user',
    action = 'store',
    metavar = '<PMA-USER>',
    help = u"user password for phpMyAdmin",
    dest = 'pma_username'
  )

  optparser.add_option(
    '-w', '--pma-passwd',
    action = 'store',
    metavar = '<PMA-PASSWORD>',
    help = u"user name for phpMyAdmin",
    dest = 'pma_password'
  )
  
  optparser.add_option(
    '-s', '--server-number',
    type= 'int',
    #default = 1,
    metavar = '<SERVER NUMBER>',
    help = u'MySQL server number(default: 1)',
    dest = 'server_number',
  )

  optparser.add_option(
    '-d', '--directory',
    action = 'store',
    metavar = '<DIRECTORY>',
    help = u"directory to save",
    dest = 'tgt_dir'
  )

  optparser.add_option(
    '-q','--quiet'
  , action = 'store_true'
  , help = u"quiet mode"
  , dest = 'quiet'
  )
  
  optparser.add_option(
    '-f', '--option-list-file',
    action = 'store',
    metavar = '<OPTION LIST FILE>',
    help = u"option list file",
    dest = 'option_file'
  )

  (options, args) = optparser.parse_args()

  # --- デフォルト
  (user, passwd) = (None, None)
  (pma_username, pma_password) = (None, None)
  server_number = 1
  tgt_dir = None
  quiet = False

  if options.option_file:
    fp = open(options.option_file, 'rb')
    _argv = []
    for line in fp:
      line = line.strip()
      mrslt = re.search('^(-\w)\s+(.*)$', line)
      if mrslt:
        _argv.append(mrslt.group(1))
        _argv.append(mrslt.group(2))
      else:
        _argv.append(line)
    fp.close()
    
    (_options, _args) = optparser.parse_args(_argv)

    # --- オプションファイルでの指定
    if _options.user is not None: user = _options.user
    if _options.passwd is not None: passwd = _options.passwd
    if _options.pma_username is not None: pma_username = _options.pma_username
    if _options.pma_password is not None: pma_password = _options.pma_password
    if _options.server_number is not None: server_number = _options.server_number
    if _options.tgt_dir is not None: tgt_dir = _options.tgt_dir
    if _options.quiet is not None: quiet = _options.quiet

  # --- ユーザ指定
  if options.user is not None: user = options.user
  if options.passwd is not None: passwd = options.passwd
  if options.pma_username is not None: pma_username = options.pma_username
  if options.pma_password is not None: pma_password = options.pma_password
  if options.server_number is not None: server_number = options.server_number
  if options.tgt_dir is not None: tgt_dir = options.tgt_dir
  if options.quiet is not None: quiet = options.quiet

  if 1 < len(args):
    exit_code = 0
    url_phpmyadmin_top = args[0]
    load_sqldump = LoadSqlDump(url_phpmyadmin_top, user=user, passwd=passwd, pma_username=pma_username, pma_password=pma_password, tgt_dir=tgt_dir, server_number=server_number, quiet=quiet)
    for db_name in args[1:]:
      if not load_sqldump.do(db_name):
        exit_code += 1
    exit(exit_code)
  else:
    optparser.print_help()
    exit(255)

#} // end of __main__

# ■ end of file
