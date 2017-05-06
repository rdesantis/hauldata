DBPA - Database Process Automation
==================================

*DBPA lets you easily automate database workflow tasks.  It's like SSIS but much easier.*

Java-based, it works with any database that has a JDBC driver.  Used extensively with MS SQL Server.  Tested with MySQL.

Full documentation coming soon at [hauldata.com](http://www.hauldata.com).

What's It Do?
-------------

Easily automates common integration, transformation, and migration tasks:

- exporting data
- importing data
- generating reports
- zipping and unzipping files
- sending email notifications including attachments
- sending and receiving files via FTP
- calling web services
- file system operations (creating directories, copying and renaming files, deleting files)

Writes and reads common file formats:

- CSV (write and read)
- XLSX (write and read)
- TXT plain text (write and read)
- Tab separated values (write only)

Easily runs multiple tasks in parallel.

Sensible, flexible error handling.

Straightforward handling of loops.

Built in scheduling.

Designed to allow easy migration of existing SSIS packages.

How Does It Work?
-----------------

OK, it's yet another scripting language.  It's a DSL for database tasks that lets you script common operations very concisely.
The syntax is keyword oriented like SQL so that database developers, DBAs, and production support people with SQL knowledge will feel comfortable.
It's also very task oriented making it easy to migrate SSIS tasks to DBPA tasks.

Here's a DBPA script to save the contents of a table to CSV file:

```
TASK SaveTable
	WRITE CSV 'mytable.csv' FROM TABLE 'mytable'
END TASK
```

Here's a script to run two stored procedures every Tuesday morning and Thursday evening, save the output to files with a date stamp in the file names, and then email them:

```
VARIABLES
	suffix VARCHAR, filename1 VARCHAR, filename2 VARCHAR
END VARIABLES

TASK WeeklyReport
	ON TUESDAY AT '6:00 AM', THURSDAY AT '6:00 PM'

	TASK SetFileNames
		SET
			suffix = '_'+ FORMAT(GETDATE(), 'yyMMdd'),
			filename1 = 'report1' + suffix + '.csv',
			filename2 = 'report2' + suffix + '.csv'
	END TASK

	TASK WriteFile1
		AFTER SetFileNames
		WRITE CSV filename1 FROM PROCEDURE 'proc1'
	END TASK

	TASK WriteFile2
		AFTER SetFileNames
		WRITE CSV filename2 FROM PROCEDURE 'proc2'
	END TASK

	TASK EmailThem
		AFTER WriteFile1 AND WriteFile2
		EMAIL FROM 'reports@yourcompany.com' TO 'report_recipients@yourcompany.com'
		SUBJECT 'Awesome Reports' BODY 'Please find the awesome reports attached'
		ATTACH filename1, filename2
	END TASK
END TASK
```

If you've written SSIS packages, think about the amount of work that would be required to build, deploy,
and schedule a simple process like this using an SSIS package and a SQL Server Agent job.
Think about the arcane mapping of input fields to output fields.

Then think about the work that would be required if you needed to add a column or rearrange the column order.
With DBPA, next week those stored procedures can write completely different formats and nothing breaks!
