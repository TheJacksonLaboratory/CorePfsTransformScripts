"""
This module contains methods that access the Galaxy database.

The assumption is that it may be easier in certain situations 
to discover data from the Galaxy database than to use the API.

For documentation on sqlite3 use see:
    https://docs.python.org/3/library/sqlite3.html

The resluts of the SQL select in getOriginalFilename is:
[
	{
		"NAME": "100-20-S.txt",
		"__index__": 0,
		"auto_decompress": "Yes",
		"dbkey": "?",
		"file_data": "/projects/galaxy_dev/database/tmp/3f5830403180d620-1700583093997-4447352",
		"file_type": "auto",
		"ftp_files": null,
		"space_to_tab": null,
		"to_posix_lines": "Yes",
		"url_paste": "",
		"uuid": null
	}
]



"""
import sqlite3
import json
import sys

def getOriginalFilename(uuid):

    con = sqlite3.connect("/projects/galaxy_dev/database/universe.sqlite")
    cur = con.cursor()

    queryStmt = """SELECT 
        job_parameter.value
    FROM
        job
        INNER JOIN
        job_parameter ON (job.id = job_parameter.job_id)
        AND command_line LIKE '%{0}%'
        AND command_line LIKE '%upload.py%'
        AND job_parameter.name = 'files';"""

#    print(queryStmt.format(uuid))

    res = cur.execute(queryStmt.format(uuid))
    res.fetchall()
#    print(len(res.fetchall()))

    for row in cur.execute(queryStmt.format(uuid)):
#        print(row)
        val = row[0]
        valLs = json.loads(val)
        valDict = valLs[0]
        valName = valDict["NAME"]
        print(valName)
        return valName

def main():
    getOriginalFilename(sys.argv[1])

if __name__ == '__main__':
   main()
