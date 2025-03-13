# [START db_Query]
def db_Query(queryexec,mysqlserver,mysqlusername,mysqlpsw):
    conn=mysqlconnect(mysqlserver,mysqlusername,mysqlpsw)
    cur=conn.cursor()
    cur.execute(queryexec)
    rows=cur.fetchall()
    conn.close()
    return rows
# [END db_Query]

# [START db_Query_filter]
def db_Query_filter(queryexec,parameters,mysqlserver,mysqlusername,mysqlpsw):
    conn=mysqlconnect(mysqlserver,mysqlusername,mysqlpsw)
    cur=conn.cursor()
    cur.execute(queryexec,parameters)
    rows=cur.fetchall()
    conn.close()
    return rows
# [END db_Query_filter]
