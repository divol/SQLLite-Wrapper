//
//  SQLiteDBWrapper.m
//
//  Rukkus
//  Created by Sanjay Waza on 9/26/12.
//  Copyright (c) 2012 Rukkus. All rights reserved.

#import "SQLiteDBWrapper.h"
#import "RukkusConstants.h"


static sqlite3   *m_database;
static NSString  *m_error ;


@implementation SQLiteDBWrapper

#pragma mark -----------------------------------------------------
#pragma mark === Constructor / Destructor Functions  ===
#pragma mark -----------------------------------------------------

// ---------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------

+ (void) createDatabase:(NSString *) _name createString:(NSString *) _createString
{
	
	if ( ![self createWithBundleDatabaseWithName:_name] )
	{
		if ( ![ self createAnEmptyDatabaseWithName:_name createStatement:_createString] )
		{
			m_error = @"Error : Failed to create the database.";
#if LOG_DB_ERRORS
			NSLog( @"%@", m_error );
#endif
		}
 	}

}

+ (BOOL)createWithBundleDatabaseWithName:(NSString*) _name
{
	// Get the main bundle for the app
	NSString *  bundleDatabaseFileName = [[NSBundle mainBundle] pathForResource:_name ofType:@"sqlite"];
	
	NSLog(@"The bundle filename is %@",bundleDatabaseFileName);
	// create the local database file name
    NSArray  *	paths = NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, YES);
    NSString *	documentsDirectory = [paths objectAtIndex:0];
    NSString *	localDatabaseFileName = [[documentsDirectory stringByAppendingPathComponent:_name] stringByAppendingString:@".sqlite"];
	
	NSFileManager * fileManager = [NSFileManager defaultManager];
	bool bLocal		= [fileManager fileExistsAtPath:localDatabaseFileName];
	bool bBundle	= [fileManager fileExistsAtPath:bundleDatabaseFileName];
	
	NSLog(@"The local FileName is %@",localDatabaseFileName);
	if ( bLocal ||  bBundle )
	{
		if ( !bLocal )
		{
			// copy the bundle database over
			if ( ![fileManager copyItemAtPath:bundleDatabaseFileName toPath:localDatabaseFileName error:nil] )
			{
#if LOG_DB_ERRORS
				NSLog( @"Error : Failed to copy database %@ over to the user directory.", _name );
#endif
				return FALSE;
			}
		}
		
		// Open a connection to the database the database. 
		if (sqlite3_open([localDatabaseFileName UTF8String], &m_database) != SQLITE_OK) 
		{
			// close the database as something has gone wrong.
			sqlite3_close(m_database);
			m_database = nil;
#if LOG_DB_ERRORS
			NSLog( @"Error : Failed to open database %@ with message '%s'.", _name, sqlite3_errmsg(m_database));
#endif
		}
		else
		{
			return TRUE;
		}
	}	
	
	return FALSE;
}

// ---------------------------------------------------------------
// create or open the local database
// ---------------------------------------------------------------

+ (BOOL) createAnEmptyDatabaseWithName:(NSString *) _name createStatement:(NSString *) _createStatement
{
	
	// create the database in the sand box's resource directory.
	NSArray  *paths = NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, YES);
	NSString *documentsDirectory = [paths objectAtIndex:0];
	NSString *localDatabaseFileName = [documentsDirectory stringByAppendingPathComponent:_name];
	
	// Open a connection to the database the database. 
	if (sqlite3_open([localDatabaseFileName UTF8String], &m_database) != SQLITE_OK) 
	{
		// close the database as something has gone wrong.
		sqlite3_close(m_database);
		m_database = nil;
		
#if LOG_DB_ERRORS
		NSLog( @"Error : Failed to open database with message '%s'.", sqlite3_errmsg(m_database));
#endif
		return FALSE;
	}
	else if ( _createStatement ) // create the database table if one is provided
	{
		// create the sql statement
		sqlite3_stmt * createStatement;
		if (sqlite3_prepare_v2(m_database, [_createStatement UTF8String], -1, &createStatement, NULL) != SQLITE_OK) 
		{
#if LOG_DB_ERRORS
			NSLog( @"Error : Failed to prepare creation statement with message '%s'.", sqlite3_errmsg(m_database));
#endif
			// close the database as something has gone wrong.
			sqlite3_close(m_database);
			m_database = nil;
			
			return FALSE;
		}
		else
		{
			// try to create the table
			int ret = sqlite3_step(createStatement);
			sqlite3_finalize(createStatement);
			
			if ( ret != SQLITE_DONE )
			{
#if LOG_DB_ERRORS
				NSLog( @"Error : Failed to execute statement with message '%s'.", sqlite3_errmsg(m_database));
#endif
				// close the database as something has gone wrong.
				sqlite3_close(m_database);
				m_database = nil;
				
				return FALSE;
			}
		}
	}
	
	return TRUE;
}


// ---------------------------------------------------------------
// Destructor
// ---------------------------------------------------------------
+(void) closeDatabase
{	
	if ( m_database )
	{
		// Close the database.
		
		if (sqlite3_close(m_database) != SQLITE_OK) 
		{
			m_error = [NSString stringWithFormat:@"Error : failed to close database with message '%s'.", sqlite3_errmsg(m_database)];
#if LOG_DB_ERRORS
			NSLog( @"%@", m_error);
#endif
		}
		m_database = nil;
	}
}

#pragma mark ---------------------------------------------------------
#pragma mark === End Constructor / Destructor Functions  ===
#pragma mark ---------------------------------------------------------

#pragma mark ---------------------------------------------------------
#pragma mark === Public Functions  ===
#pragma mark ---------------------------------------------------------

// ---------------------------------------------------------------
// execute a single sql statement and load to a Mutable Array
// ---------------------------------------------------------------

+ (BOOL) executeAndLoadToArray:(NSString *) _sql results:(NSMutableArray *) _results 
{ 
	if ( m_database )
	{ 
		sqlite3_stmt * sqlStatement = nil;
		if (sqlite3_prepare_v2(m_database, [_sql UTF8String], -1, &sqlStatement, NULL) != SQLITE_OK) 
		{
			m_error = [NSString stringWithFormat:@"Error : in Sql %@, failed to prepare statement with message '%s'.", _sql, sqlite3_errmsg(m_database)];
#if LOG_DB_ERRORS
			NSLog( @"%@", m_error);
#endif
		}
		else
		{			
			// step over each of the rows and grab the data.
			while (sqlite3_step(sqlStatement) == SQLITE_ROW) 
			{
				if ( _results )
				{
					int count = sqlite3_data_count(sqlStatement);
					
					// grab the row data and place it into a dictionary.
					NSMutableDictionary * row = [NSMutableDictionary dictionaryWithCapacity:count];
				    for ( int i=0; i<count; i++ )
					{
						NSString * columnName = [NSString stringWithUTF8String:(char *)sqlite3_column_name(sqlStatement, i)];
						switch ( sqlite3_column_type(sqlStatement, i) )
						{
							case SQLITE_INTEGER:
							{
								[row setObject:[NSNumber numberWithInt:sqlite3_column_int(sqlStatement, i)] forKey:columnName];
								break;
							}
							case SQLITE_FLOAT:
							{
								[row setObject:[NSNumber numberWithDouble:sqlite3_column_double(sqlStatement, i)] forKey:columnName];
								break;
							}
							case SQLITE_BLOB:
							{
								const void * blob = sqlite3_column_blob(sqlStatement, i);
								int bytes = sqlite3_column_bytes(sqlStatement, i);
								
								[row setObject:[NSData dataWithBytes:blob length:bytes]  forKey:columnName];
								break;
							}
							case SQLITE_NULL:
							{
								// do nothing as the dictionary will return nil when the column is requested
								break;
							}
							case SQLITE_TEXT:
							{
								[row setObject:[NSString stringWithUTF8String:(char *)sqlite3_column_text(sqlStatement, i)] forKey:columnName];
								break;
							}
						}
					}
					
					// add a row into the results object
					[_results addObject:row];
					
				}
				else
				{
					break;
				}
			}
			
			// finalize the sql statment to make sure it executed correctly
			if ( sqlite3_finalize(sqlStatement) == SQLITE_OK )
			{
				return TRUE;
			}
			else
			{
				m_error = [NSString stringWithFormat:@"Error : Failed to execute statement with message '%s'.", sqlite3_errmsg(m_database)];
#if LOG_DB_ERRORS
				NSLog( @"%@", m_error);
#endif
			}
		}
	}
	
	return FALSE;
}

// ------------------------------------------------------------------
// execute a single sql statement and loads to a Mutable Dictionary
// ------------------------------------------------------------------

+ (BOOL) executeAndLoadToDictionary:(NSString *)_sql result:(NSMutableDictionary *) _result
{
	if ( m_database )
	{
		sqlite3_stmt * sqlStatement = nil;
		if (sqlite3_prepare_v2(m_database, [_sql UTF8String], -1, &sqlStatement, NULL) != SQLITE_OK) 
		{
			m_error = [NSString stringWithFormat:@"Error : in Sql %@, failed to prepare statement with message '%s'.", _sql, sqlite3_errmsg(m_database)];
#if LOG_DB_ERRORS
			NSLog( @"%@", m_error);
#endif
		}
		else
		{			
			// step over each the first rows and grab the data.
			while (sqlite3_step(sqlStatement) == SQLITE_ROW) 
			{
				if ( _result )
				{
					int count = sqlite3_data_count(sqlStatement);
					
					// grab the row data and place it into a dictionary.
				    for ( int i=0; i<count; i++ )
					{
						NSString * columnName = [NSString stringWithUTF8String:(char *)sqlite3_column_name(sqlStatement, i)];
						NSLog( @"%@", columnName);
						switch ( sqlite3_column_type(sqlStatement, i) )
						{
							case SQLITE_INTEGER:
							{
								[_result setObject:[NSNumber numberWithInt:sqlite3_column_int(sqlStatement, i)] forKey:columnName];
								break;
							}
							case SQLITE_FLOAT:
							{
								[_result setObject:[NSNumber numberWithDouble:sqlite3_column_double(sqlStatement, i)] forKey:columnName];
								break;
							}
							case SQLITE_BLOB:
							{
								const void * blob = sqlite3_column_blob(sqlStatement, i);
								int bytes = sqlite3_column_bytes(sqlStatement, i);
								
								[_result setObject:[NSData dataWithBytes:blob length:bytes]  forKey:columnName];
								break;
							}
							case SQLITE_NULL:
							{
								// do nothing as the dictionary will return nil when the column is requested
								break;
							}
							case SQLITE_TEXT:
							{
								[_result setObject:[NSString stringWithUTF8String:(char *)sqlite3_column_text(sqlStatement, i)] forKey:columnName];
								break;
							}
						}
					} 
				}
			
				break;
			}
			
			// finalize the sql statment to make sure it executed correctly
			if ( sqlite3_finalize(sqlStatement) == SQLITE_OK )
			{
				return TRUE;
			}
			else
			{
				m_error = [NSString stringWithFormat:@"Error : Failed to execute statement with message '%s'.", sqlite3_errmsg(m_database)];
#if LOG_DB_ERRORS
				NSLog( @"%@", m_error);
#endif
			}
		}
	}
	
	return TRUE;
}

// ------------------------------------------------------------------
// execute a single sql statement mostly for sql execs
// ------------------------------------------------------------------

+ (BOOL) executeDDL:(NSString *) _sql
{
	if ( m_database )
	{
		sqlite3_stmt * sqlStatement = nil;
		if (sqlite3_exec(m_database, [_sql UTF8String], NULL,NULL,NULL) != SQLITE_OK) 
		{
			m_error = [NSString stringWithFormat:@"Error : in Sql %@, failed to execute statement with message '%s'.", _sql, sqlite3_errmsg(m_database)];
#if LOG_DB_ERRORS
			NSLog( @"%@", m_error);
#endif
		}
		else
		{			
			// finalize the sql statment to make sure it executed correctly
			if ( sqlite3_finalize(sqlStatement) == SQLITE_OK )
			{
				return TRUE;
			}
			else
			{
				m_error = [NSString stringWithFormat:@"Error : Failed to execute statement with message '%s'.", sqlite3_errmsg(m_database)];
#if LOG_DB_ERRORS
				NSLog( @"%@", m_error);
#endif
			}
		}
	}
	
	return FALSE;
}

+ (BOOL) executeBulkInsertDDL:(NSArray *)data tableName:(NSString *)tablename columnList:(NSString *)columnList {
    if ( m_database )
    {
        static sqlite3_stmt *insert_category_query = nil;
        
        NSInteger insertCount=0;
        NSLog(@"array count %d",[data count]);
        NSDate *date=[NSDate date];
        NSDateFormatter *formatter = [[NSDateFormatter alloc] init];
        [formatter setDateFormat: @" HH:mm:ss"];
        [formatter setTimeZone:[NSTimeZone timeZoneWithName:@"..."]];
        NSString *stringFromDate = [formatter stringFromDate:date];
        NSLog(@"Parsing started at :-  %@",stringFromDate);
        
        NSMutableDictionary *keysDic=[data objectAtIndex:0];
        NSArray *keysArray=[keysDic allKeys];
        NSLog(@"The keys Array is %@",keysArray);
        
        sqlite3_stmt *begin_transaction_stmt;
        const char *beginTrans = "BEGIN EXCLUSIVE TRANSACTION";
        if (sqlite3_prepare_v2(m_database, beginTrans, -1, &begin_transaction_stmt, NULL) != SQLITE_OK) {
            NSAssert1(0, @"Error: Failed to prepare exclusive transaction: '%s'.", sqlite3_errmsg(m_database));
        }
        if (SQLITE_DONE != sqlite3_step(begin_transaction_stmt)) {
            NSAssert1(0, @"Error: Failed to step on begin_transaction_stmt: '%s'.", sqlite3_errmsg(m_database));
        }
        
        sqlite3_finalize(begin_transaction_stmt);
       
       // NSMutableDictionary *dataDic=[[NSMutableDictionary alloc] init];
        
        for (int i=0;i<[data count]; i++) {
             NSMutableDictionary *dataDic =[data objectAtIndex:i];
             insertCount++;
             NSMutableString * insertQuery=[NSMutableString stringWithFormat:@"insert or replace into %@(%@) values(",tablename,columnList];
            
                for(int i=0;i<[keysArray count];i++)
                {
                    if(i==[keysArray count]-1)
                    {
                        [insertQuery appendString:[NSString stringWithFormat:@"?);"]];
                    }else
                    {
                        [insertQuery appendString:[NSString stringWithFormat:@"?,"]];
                    }
                }
          
                  if (sqlite3_prepare_v2(m_database, [insertQuery UTF8String], -1, &insert_category_query, NULL) != SQLITE_OK) {
                    NSAssert1(0, @"Error: failed to prepare insert_category_query: '%s'.", sqlite3_errmsg(m_database));
                      return FALSE;
                } else
                {
                    NSLog(@"insert_index %d",i);
                   // NSLog(@"sql problem occured with: %s", [insertQuery UTF8String]);
                    {
                        for(int j=0;j<[keysArray count];j++)
                        {
                            NSString *key=[keysArray objectAtIndex:j];
                          //  NSLog(@"key %@",key);
                            NSString *value=[NSString stringWithFormat:@"%@",[dataDic objectForKey:key]];
                          //  NSLog(@"value %@",value);
                            sqlite3_bind_text(insert_category_query, j+1, [value UTF8String], -1, SQLITE_TRANSIENT);
                        }
                        sqlite3_step(insert_category_query);
                        sqlite3_clear_bindings(insert_category_query);
                        sqlite3_reset(insert_category_query);
                    }
                    
                    sqlite3_finalize(insert_category_query);
                   /*
                    // finalize the sql statment to make sure it executed correctly
                    if ( sqlite3_finalize(insert_category_query) == SQLITE_OK )
                    {
                        return TRUE;
                    }
                    else
                    {
                        m_error = [NSString stringWithFormat:@"Error : Failed to execute statement with message '%s'.", sqlite3_errmsg(m_database)];
#if LOG_DB_ERRORS
                        NSLog( @"%@", m_error);
#endif
                    } 
*/
                }
        }
                         
        sqlite3_stmt *end_transaction_stmt;
        const char *endTrans = "COMMIT";
        if (sqlite3_prepare_v2(m_database, endTrans, -1, &end_transaction_stmt, NULL) != SQLITE_OK) {
            NSAssert1(0, @"Error: failed to commit transaction: '%s'.", sqlite3_errmsg(m_database));
            return FALSE;
        }
        if (SQLITE_DONE != sqlite3_step(end_transaction_stmt)) {
            NSAssert1(0, @"Error: Failed to step on end_transaction_stmt: '%s'.", sqlite3_errmsg(m_database));
            return FALSE;
        }
               // finalize the sql statment to make sure it executed correctly
                        if ( sqlite3_finalize(end_transaction_stmt) == SQLITE_OK )
                        {
                                return TRUE;
                        }
                        else
                        {
                                m_error = [NSString stringWithFormat:@"Error : Failed to execute statement with message '%s'.", sqlite3_errmsg(m_database)];
#if LOG_DB_ERRORS
                                NSLog( @"%@", m_error);
#endif
                        }

        // sqlite3_finalize(end_transaction_stmt);
     
    } //end of if db

        return TRUE;
 }



+ (NSString *) escapeString:(NSString *)string {
	NSRange range = NSMakeRange(0, [string length]);
	return [string stringByReplacingOccurrencesOfString:@"'" withString:@"''" options:NSCaseInsensitiveSearch range:range];
}

@end
