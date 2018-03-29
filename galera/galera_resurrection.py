#!/usr/bin/python
# -*- coding: utf-8

import requests, ConfigParser, os, mysql.connector, sys, logging, time, psutil, signal
from socket import gethostname
from subprocess import call


my_hostnname = gethostname()
consul_health_url = "http://localhost:8500/v1/health/checks/poc-galera"
consul_last_commited_url = "http://localhost:8500/v1/health/checks/galera-last-commited"
soft_bootstrap_retry = 3

logging.basicConfig(format = "%(levelname)-8s [%(asctime)s] %(message)s", level = logging.DEBUG, filename = "/var/log/galera_resurrection.log")

logging.debug("Get mysql credentials from ~/.my.cnf")
try:
  my_cnf = ConfigParser.ConfigParser()
  my_cnf.read(os.path.expanduser('~/.my.cnf'))
  my_user = my_cnf.get('mysql', 'user')
  my_pass = my_cnf.get('mysql', 'password')
except ConfigParser.Error as err:
  logging.error("Can't read my.cnf file: %s" % err)
  sys.exit(1)

logging.debug("Get health data from consul server")
try:
  req_data = requests.get(consul_health_url).json()
  req_last_commited_data = requests.get(consul_last_commited_url).json()
  max_last_commited_val = max([int(item['Output'].split()[1]) for item in req_last_commited_data if 'wsrep_last_committed' in item['Output']])
  nodes_last_commited_max = [item['Node'] for item in req_last_commited_data if int(item['Output'].split()[1]) == max_last_commited_val]
  statuses = [item['Status'] for item in req_data]
  if len(nodes_last_commited_max) > 0:
    last_modified  = max([item['ModifyIndex'] for item in req_data if item['Node'] in nodes_last_commited_max])
  else:
    last_modified  = max([item['ModifyIndex'] for item in req_data])
  bootstrap_node = (item['Node'] for item in req_data if item['ModifyIndex'] == last_modified).next()
except requests.exceptions.ConnectionError as err:
  logging.error("Can't connect to consul server: %s" % err)
  sys.exit(1)
except:
  logging.error("Can't receive data from consul: %s" % sys.exc_info()[0])
  sys.exit(1)

def bootstrap():
  logging.info("Try to start mysqld service with bootstrap-pxc")
  f = open("/var/lib/mysql/data/grastate.dat", 'r')
  filedata = f.read()
  f.close()
  newdata = filedata.replace("safe_to_bootstrap: 0", "safe_to_bootstrap: 1")
  f = open("/var/lib/mysql/data/grastate.dat",'w')
  f.write(newdata)
  f.close()
  cmd = ['/usr/bin/timeout', '120', '/sbin/service', 'mysql', 'restart-bootstrap']
  try:
    call(cmd)
  except:
    logging.error("Can't start mysql service with restart-bootstrap: %s" % sys.exc_info()[0])
    sys.exit(1)

def do_hard_bootstrap():
  logging.info("Try to hard bootstrap")
  mysqld_pid = [process.pid for process in psutil.process_iter() if process.name == 'mysqld']
  if len(mysqld_pid) != 0:
    logging.info("Mysqld service is running with pid %s. Try to stop it." % mysqld_pid[0])
    cmd = ['/usr/bin/timeout', '60', '/sbin/service', 'mysql', 'stop']
    try:
      if call(cmd) == 0:
        logging.info("Mysqld service stopped successfully")
        bootstrap()
      else:
        raise
    except:
      logging.error("Can't stop service in time. Try to kill mysqld")
      mysqld_pid = [process.pid for process in psutil.process_iter() if process.name == 'mysqld']
      if len(mysqld_pid) != 0:
        os.kill(mysqld_pid[0], signal.SIGKILL)
      bootstrap()
  else:
    logging.info("Mysqld process not found")
    bootstrap()

def do_soft_bootstrap():
  logging.info("Trying to bootstrap it softly.")
  try:
    cnx = mysql.connector.connect(host='localhost', user=my_user, password=my_pass)
    cursor = cnx.cursor()
    cursor.execute("SHOW GLOBAL STATUS LIKE 'wsrep_ready'")
    raw = cursor.fetchone()
    if raw[1] != 'ON':
      logging.info("Trying to bootstrap galera with wsrep_provider_options")
      cursor.execute("SET GLOBAL wsrep_provider_options='pc.bootstrap=YES'")
      result = False
    else:
      logging.info("Galera is ready")
      result = True
    cursor.close()
    cnx.close()
  except mysql.connector.Error as err:
    logging.error("Can't connetct to mysql server: $s" % err)
    result = False
  except:
    logging.error("Can't do soft bootstrap: %s" % sys.exc_info()[0])
    result = False
  finally:
    return result

if 'passing' not in statuses:
  if my_hostnname == bootstrap_node:
    logging.info("It seems like galera cluster is totally fucked up.")
    for attempt_number in range(soft_bootstrap_retry):
      print "Attempt:", attempt_number
      if do_soft_bootstrap():
        break
      time.sleep(5)
    if attempt_number + 1 >= soft_bootstrap_retry:
      do_hard_bootstrap()
else:
  logging.info("All fine!")
