#ifndef LOG_H_
#define LOG_H_

#include "global.h"

/** Log Level Part **/
#define DISABLE 4 //Disable - system is unusable, and will aborted. For example: Memory Allocation failed.
#define EMERG 3 //Emergencies - system is unusable, and will aborted. For example: Memory Allocation failed.
#define ISSUE 2 //Some bad thing has occurs, but the situation is recoverable. For example: the default region port has been taken.
#define INFO 1 //Some important event happens, need to record it. For example: the memstore is flushing.
#define DEBUG 0 //Store all performance and command info. For example: the time of command execution

public void setup_logging(int log_level, char* log_file_path);

public void logg(int level, const char *fmt, ...);

#endif /* LOG_H_ */
