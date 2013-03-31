//
//  SQLiteDBWrapper.h
//
//  Rukkus
//  Created by Sanjay Waza on 9/26/12.
//  Copyright (c) 2012 Rukkus. All rights reserved.



#import <Foundation/Foundation.h>
#import <UIKit/UIKit.h>
#import <sqlite3.h>

@interface SQLiteDBWrapper : NSObject {

}

+ (void) createDatabase:(NSString *) _name createString:(NSString *) _createString;

+ (BOOL) createWithBundleDatabaseWithName:(NSString*) _name;
+ (BOOL) createAnEmptyDatabaseWithName:(NSString *) _name createStatement:(NSString *) _createStatement;
+ (void) closeDatabase;
+ (BOOL) executeAndLoadToArray:(NSString *) _sql results:(NSMutableArray *) _results;
+ (BOOL) executeAndLoadToDictionary:(NSString *)_sql result:(NSMutableDictionary *) _result;
+ (BOOL) executeDDL:(NSString *)_sql;
+ (BOOL) executeBulkInsertDDL:(NSArray *)data tableName:(NSString *)tablename columnList:(NSString *)columnList;
+ (NSString *) escapeString:(NSString *)string;
@end
