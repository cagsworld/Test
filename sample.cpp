//////////////////////////////////////////////////////////
// import.cpp
// Function:
//      Uses FTP to get BCP files from remote server and
//      bulk load records into SQL Tables 
//////////////////////////////////////////////////////////

// Include db-libary headers
#include <sybfront.h> 
#include <sybdb.h> 
#include <syberror.h>

// Include file io routines
#include <time.h>
#include <fcntl.h>
#include <io.h>
#include <fstream.h>
#include <windows.h>
#include <errno.h>

// Include FTP library header
#include <winftp32.h>

// A few defines
#define ERROR_PREFIX   szSystem << " " << TimeStamp() << " Error from import.exe: "
#define WARNING_PREFIX szSystem << " " << TimeStamp() << " Alert from import.exe: "
#define INFO_PREFIX    szSystem << " " << TimeStamp() << " Info  from import.exe: "
#define MAXLOGSIZE 200 //  200 bytes, change to 10 mb for production
#define MAXERRSIZE 100 //  100 bytes, change to  1 mb for production

// Function prototypes
BOOL OpenLogFiles();
BOOL CheckArgs(int, char **);
BOOL DBConnect(char *szServer, char *szUser, char *szPswd);
int BCPInRecords(ifstream bcpInfile, char *szExtSystem);
BOOL ftp_exist(int hndl, CHAR * szFile);
void ShowUsage();
char *TimeStamp();
long FileSize(char *);
int err_handler(DBPROCESS *dbproc, int severity, int dberr,
				int oserr, char *dberrstr, char *oserrstr);
int msg_handler(DBPROCESS *dbproc, DBINT msgno, int msgstate, int severity,
				char *msgtext, char *srvname, char *procname, DBUSMALLINT line);

// Module level vars
ofstream logfile, errfile;	// handle to log and error file used by many functions
char szTimeStamp[19];		// used to stamp time in messages to log and error file
char *szSystem;				// used for system abrev in messages to log and error file
char buffer[256];			// utility string buffer
LOGINREC *login;			// the login information
DBPROCESS *dbproc;			// the connection with SQL Server
extern int errno;


void main(int argc, char *argv[])
{
	ifstream bcpInfile;		// local import file holding records to bcp to x_import table
	int	hndl;				// file handle
	char szXfrFile[128];	// holds name of delimited import file
	char szTmpFile[128];	// holds name of renamed delimited import file	
	char szXfrLocalFile[128]; // holds name of delimited import file with local dir prepended
	int	 intBcpCount;		// number of rows bcp'd

	// Open import log and import error log files
	if ( !OpenLogFiles() )
		return;

	if ( argc >= 7 )
		szSystem = argv[7];
	else 
		szSystem = "";

	// Check if user wants help or if incorrect args entered
	if ( !CheckArgs(argc, argv) )
		return;

	// Create transfer filenames
	wsprintf(szXfrFile, "%s_cags.xfr", argv[7] );
	wsprintf(szTmpFile, "%s_cags.yyy", argv[7] ); 

	// Connect to database and init db structures
	if ( !DBConnect(argv[1], argv[2], argv[3]) )
		return;

	//
	// Check if local wms_cags.xfr exists to BCP records into import table
	//
	wsprintf(szXfrLocalFile, "%s%s", "./transfer/", szXfrFile ); 
	if ( FileSize(szXfrLocalFile) > -1 )  // file exists
	{
		// Open local wms_cags.xfr 
		bcpInfile.open(szXfrLocalFile, ios::nocreate, filebuf::sh_none);
		if ( !bcpInfile.is_open() )
		{
			// Failed open so get out and retry on next run
			errfile << ERROR_PREFIX  << "failed to open bcp input file" << endl;
			return;
		}
		else
		{
			// Call bcp routines to move data from import file into x_import table
			// Note: If migrated to another RDBMS most changes will be to this function
			if ( (intBcpCount = BCPInRecords(bcpInfile, argv[7])) != -1 )
			{
				// Delete local import file now that its records are in import table
				bcpInfile.close();
				f_deletefile(szXfrLocalFile);
				logfile << INFO_PREFIX << "successfull BCP of " << intBcpCount
						<< " records into import table" << endl;
			}
			else
			{
				// Failed bcp so don't delete and get out to retry on next run
				bcpInfile.close();
				errfile << ERROR_PREFIX  << "failed BCP of import file to import table" << endl;
				return;
			}
		}
	}
	else
	{
		if ( FileSize(szXfrLocalFile) == -1 )
			logfile << WARNING_PREFIX << "no records to import from " << szXfrLocalFile << " this run" << endl;
		else
			errfile << ERROR_PREFIX << "error opening local import file " << szXfrLocalFile << endl;
	}

	//
	// Logon to ftp server to get remote wms_cags.xfr to be bcp'd next import run
	//
	hndl = ftp_login(argv[4], argv[5], argv[6], "");
	if ( hndl == -1 )
	{
		errfile << ERROR_PREFIX << "failed ftp_login" << endl;
		return;
	}

	// Set current remote transfer directory
	if ( ftp_cd(hndl, argv[8]) == -1 )
	{
		errfile << ERROR_PREFIX << "failed ftp_cd to " << argv[8] << endl;
		return;
	}

	// Check for no left over records from prior import and new records to import
	if ( !ftp_exist(hndl, szTmpFile) && ftp_exist(hndl, szXfrFile) )
	{
		// If so, then rename prior to ftp_get to prevent contention with remote site
		if ( ftp_rename(hndl, szXfrFile, szTmpFile) == -1 )
		{
			// ftp_rename failed so just log message and try again next invocation
			errfile << ERROR_PREFIX << "failed ftp_remame of " 
					<< szXfrFile << " to " << szTmpFile
					<< " will retry next run" << endl;
			return;
		}
	}

	// Check for either left over records or new records in tmp file to import	
	if ( ftp_exist(hndl, szTmpFile) )
	{
		// If so, then ftp them to local directory
		if ( ftp_get(hndl, szTmpFile, szXfrLocalFile) == -1 )
		{
			// ftp_get failed so do nothing here and retry next invovation
			errfile << ERROR_PREFIX << "failed ftp_get of " << szTmpFile 
					<< " will retry next run" << endl;
		}
		else
		{
			if ( ftp_delete(hndl, szTmpFile) == -1 )
			{
				// ftp_delete failed so delete local to prevent re-importing next invocation
				f_deletefile(szXfrLocalFile);
				errfile << ERROR_PREFIX << "failed ftp_delete of " << szTmpFile << endl;
			}
			else
			{
				// successfull FTP
				logfile << INFO_PREFIX << "successfull FTP from remote site to " 
						<< szXfrLocalFile << endl;
			}
		}
	}						
	else
			logfile << WARNING_PREFIX << "no records to ftp from remote site this run" << endl;

	// Close opened objects
	if ( !logfile.is_open() )
		logfile.close();
	if ( !errfile.is_open() )
		errfile.close();
	dbexit();		// close database connection
	ftp_quit(hndl); // close ftp connection
}


BOOL OpenLogFiles()
{
	// Open import log file
	if ( FileSize("./log/import.log") > MAXLOGSIZE )
		// Truncate before writing
		logfile.open("./log/import.log", ios::out);
	else
		// Append to existing
		logfile.open("./log/import.log", ios::app);
	if ( !logfile.is_open() )
	{
		cerr << "Could not open 'import.log'" << endl;
		return FALSE;
	}

	// Open import error file
	if ( FileSize("./log/import.err") > MAXERRSIZE )
		// Truncate before writing
		errfile.open("./log/import.err", ios::out);
	else
		// Append to existing
		errfile.open("./log/import.err", ios::app);
	if ( !errfile.is_open() )
	{
		cerr << "Could not open 'import.err'" << endl;
		return FALSE;
	}
	return TRUE;
}


BOOL DBConnect(char *szServer, char *szUser, char *szPswd)
{
	// Initialize DB-Library
	if ( dbinit() == FAIL ) 
		return FALSE;

	// Install user-supplied error-handling and message-handling
	dberrhandle((EHANDLEFUNC)err_handler);
	dbmsghandle((MHANDLEFUNC)msg_handler);

	// Allocate and init LOGINREC structure used to open a connection to SQL Server
	login = dblogin();
	DBSETLUSER(login, szUser); // "sa"
	DBSETLPWD(login, szPswd);  // ""
	DBSETLAPP(login, "cags_import_bcp");

	// Enable bulk copy for this connection
	BCP_SETL(login, TRUE);
	
	// Get a connection to the database.
	if ((dbproc = dbopen(login, szServer)) == (DBPROCESS *) NULL)   // "cgserver"
	{
		errfile << ERROR_PREFIX << "can't connect to server" << endl;
		return FALSE;
	}

	// Make cags the current database
	if ( dbuse(dbproc, "cags") ==  FAIL )
	{
		errfile << ERROR_PREFIX << "can't make cags current database." << endl;
		return FALSE;
	}
	
	return TRUE;
}


int BCPInRecords(ifstream bcpInfile, char *szExtSystem)
{
	int intImportCount = 0;

	// Get current server datetime
	DBDATETIME dtCurDateTime;
	dtCurDateTime.dtdays = 0;
	dbcmd(dbproc, "select getdate()");
	dbsqlexec(dbproc);
	if (dbresults(dbproc) == SUCCEED) 
		if (dbnextrow(dbproc) != NO_MORE_ROWS)
			dbconvert(dbproc, SYBDATETIME, (dbdata(dbproc, 1)), 
					(DBINT)-1, SYBDATETIME, (BYTE*)&dtCurDateTime, (DBINT)-1);
	if ( dtCurDateTime.dtdays == 0 )
		return -1;

	// Call bcp_init
	if ( bcp_init(dbproc, "cags..x_import", NULL, "bcp_err.out", DB_IN) != SUCCEED )
	{
		errfile << ERROR_PREFIX << "failed bcp_init" << endl;
		return -1;
	}

	bcp_bind(dbproc, (BYTE*)szExtSystem, 0, -1, (BYTE*)"", 1, SYBCHAR, 1);
	bcp_bind(dbproc, (BYTE*)&dtCurDateTime, 0, -1, NULL, 0, SYBDATETIME, 2);
	bcp_bind(dbproc, (BYTE*)&intImportCount, 0, -1, NULL, 0, SYBINT2, 3);
	bcp_bind(dbproc, (BYTE*)buffer, 0, -1, (BYTE*)"", 1, SYBVARCHAR, 4);

	while ( !bcpInfile.eof() )
	{
		bcpInfile.getline(buffer, 255);
		// cout << buffer << endl;
		intImportCount++;
		
		// Bulk copy it into the database */
		bcp_sendrow(dbproc);
	}

	// Close the bulk copy process so all the changes are committed
	return bcp_done(dbproc);		
}



BOOL ftp_exist(int hndl, CHAR * szFile)
{
	if ( (ftp_ls(hndl, szFile, "NUL") == -1) && (ftp_replycode(hndl) == 550) )
		 return FALSE;
	else 
		return TRUE;
}


BOOL CheckArgs(int argc, char *argv[])
{
	// User requesting help?
	if ( argc > 0 ) 
	{
		if ((lstrcmp(argv[1], "/?") == 0) ||
			(lstrcmp(argv[1], "?") == 0) ||
			(lstrcmp(argv[1], "-?") == 0) ||
			(lstrcmp(argv[1], "help") == 0) ||
			(lstrcmp(argv[1], "/help") == 0) ||
			(lstrcmp(argv[1], "/HELP") == 0) ||
			(lstrcmp(argv[1], "HELP") == 0))
		{
			ShowUsage();
			return FALSE;
		}
	}

	// Correct number of parameters specified on command line??
	if ( argc != 9 )
	{
		cout << "Error: Wrong number of paramters" << endl << endl;
		errfile << ERROR_PREFIX  << "wrong number of command line parameters" << endl;
		ShowUsage();
		return FALSE;
	}

	return TRUE;
}


void ShowUsage()
{
	cout << "Syntax:" << endl << "  import" << endl ;
    cout << "  <db_serv> <db_user> <db_pswd> <ftp_serv> <ftp_user> <ftp_pswd> <ext_sys> <ftp_alias_dir>" << endl << endl;
	cout << "For Example:" << endl;
	cout << "  import my_server, my_user, my_pswd, ftpvax cags_user cags_pswd cags" << endl << endl;
	cout << "Assumptions: program is invoked from interfaces dir" << endl;
}


char *TimeStamp()
{
	char dbuffer [9];
	char tbuffer [9];

	_strdate(dbuffer);
	_strtime(tbuffer);
	wsprintf(szTimeStamp, "%s %s", dbuffer, tbuffer);
	return szTimeStamp;
}


long FileSize(char * szFile)
{
	int fh;
	long fl;

	fh = _open( szFile, _O_RDONLY );
	if( fh == -1 )
	{
		if ( errno == ENOENT ) 
			return -1; // no file
		else
			return -2; // open error
	}
	else
	{
		fl = _filelength( fh );
		_close( fh );
		return fl;
	}

}


int err_handler(DBPROCESS *dbproc, int severity, int dberr,
				int oserr, char *dberrstr, char *oserrstr)
{
	if ((dbproc == NULL) || (DBDEAD(dbproc)))
		return(INT_EXIT);
	else 
	{
		errfile << ERROR_PREFIX << "DB-Library error: " << dberrstr << endl;

		if (oserr != DBNOERR)
			errfile << ERROR_PREFIX << "Operating-system error: " << oserrstr << endl;
		return(INT_CANCEL);
	}
}


int msg_handler(DBPROCESS *dbproc, DBINT msgno, int msgstate, int severity,
				char *msgtext, char *srvname, char *procname, DBUSMALLINT line)
{
	// If it's a database change message, we'll ignore it.
	// Also ignore language change message.
	if (msgno == 5701 || msgno == 5703)
		return(0);

	errfile << ERROR_PREFIX << "Msg " << msgno 
		    << ", Level " << severity
			<< ", State "<< msgstate << endl;

	if (strlen(srvname) > 0)
		errfile << "Server " << srvname;
	if (strlen(procname) > 0)
		errfile << " Procedure " << procname;
	if (line > 0)
		errfile << " Line " << line << endl;
	errfile << msgtext << endl;

	return(0);
}
