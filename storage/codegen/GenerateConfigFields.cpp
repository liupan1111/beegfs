#include <beegfs-codegen/ConfigCodegen.h>

int main()
{
   ConfigFieldsGenerator gen;

   gen.field("connInterfacesFile",       "", CFGTYPE_STRING);
   gen.field("connInterfacesList",       "", CFGTYPE_STRING);
   gen.field("storeStorageDirectory",    "", CFGTYPE_STRING, CFGFLAG_IS_LIST);
   gen.field("storeFsUUID",              "", CFGTYPE_STRING, CFGFLAG_IS_LIST);
   gen.field("storeAllowFirstRunInit",   "true", CFGTYPE_BOOL);
   gen.field("tuneNumStreamListeners",        "1", CFGTYPE_UINT);
   gen.field("tuneNumWorkers",                "8", CFGTYPE_UINT);
   gen.field("tuneWorkerBufSize",             "4m", CFGTYPE_INT64, CFGFLAG_HUMAN_READABLE_SIZE);
   gen.field("tuneProcessFDLimit",            "50000", CFGTYPE_UINT);
   gen.field("tuneWorkerNumaAffinity",        "false", CFGTYPE_BOOL);
   gen.field("tuneListenerNumaAffinity",      "false", CFGTYPE_BOOL);
   gen.field("tuneListenerPrioShift",         "-1", CFGTYPE_INT);
   gen.field("tuneBindToNumaZone",            "", CFGTYPE_INT, CFGFLAG_EMPTY_IS_MINUS1);
   gen.field("tuneFileReadSize",              "32k", CFGTYPE_INT64, CFGFLAG_HUMAN_READABLE_SIZE);
   gen.field("tuneFileReadAheadTriggerSize",  "4m", CFGTYPE_INT64, CFGFLAG_HUMAN_READABLE_SIZE);
   gen.field("tuneFileReadAheadSize",         "0", CFGTYPE_INT64, CFGFLAG_HUMAN_READABLE_SIZE);
   gen.field("tuneFileWriteSize",             "128k", CFGTYPE_INT64, CFGFLAG_HUMAN_READABLE_SIZE);
   gen.field("tuneFileWriteSyncSize",         "0", CFGTYPE_INT64, CFGFLAG_HUMAN_READABLE_SIZE);
   gen.field("tuneUsePerUserMsgQueues",       "false", CFGTYPE_BOOL);
   gen.field("tuneDirCacheLimit",             "1024", CFGTYPE_UINT);
   gen.field("tuneEarlyStat",                 "false", CFGTYPE_BOOL);
   gen.field("tuneNumResyncSlaves",           "12", CFGTYPE_UINT);
   gen.field("tuneNumResyncGatherSlaves",     "6", CFGTYPE_UINT);
   gen.field("tuneUseAggressiveStreamPoll",   "false", CFGTYPE_BOOL);
   gen.field("tuneUsePerTargetWorkers",       "true", CFGTYPE_BOOL);
   gen.field("tuneAsyncIOBackend",             "libaio", CFGTYPE_STRING);
   gen.field("tuneChunkBalanceQueueLimit",    "100000", CFGTYPE_UINT);
   gen.field("quotaEnableEnforcement",        "false", CFGTYPE_BOOL);
   gen.field("quotaDisableZfsSupport",        "false", CFGTYPE_BOOL);
   gen.field("sysResyncSafetyThresholdMins",  "10", CFGTYPE_INT64);
   gen.field("sysTargetOfflineTimeoutSecs",   "180", CFGTYPE_UINT);
   gen.field("runDaemonized",            "false", CFGTYPE_BOOL);
   gen.field("pidFile",                  "", CFGTYPE_STRING);
   gen.field("tuneResyncQueueLimit", "250000", CFGTYPE_UINT);

   const char *dirpath = "../generated";  // relative to build directory
   const char *classname = "StorageConfigFields";

   generate_config_sources(&gen, dirpath, classname);

   return 0;
}
