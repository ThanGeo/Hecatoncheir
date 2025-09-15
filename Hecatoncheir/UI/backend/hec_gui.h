#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <../../API/Hecatoncheir.h>
 

using namespace std;

extern vector<string> hosts;
extern string pointsPath;
extern int pointDatasetID;
extern int ret;
extern string queriesPath;
extern vector<hec::Query *> batch;
extern unordered_map<int, hec::QResultBase *> results;
extern int k;
extern double start;
extern double total_time;

void printUsage();
vector<string> parseArguments(int argc, char* argv[]);
string getArgumentValue(const vector<string>& args, const string& flag);
int handleInitCommand(const vector<string>& args);
int handlePrepareCommand(const vector<string>& args);
int handleExecuteCommand(const vector<string>& args);
int handleTerminateCommand(const vector<string>& args);