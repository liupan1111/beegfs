#include <beegfs-codegen/ConfigCodegen.h>

int main()
{
   ConfigFieldsGenerator gen;

   // re-definitions
   gen.override_default("cfgFile", "");
   gen.override_default("connMaxInternodeNum", "16");

   // own definitions

   gen.field("connInterfacesFile",           "",       CFGTYPE_STRING);
   gen.field("connInterfacesList",           "",       CFGTYPE_STRING); // comma-separated list
   gen.field("storeMetaDirectory",           "",       CFGTYPE_STRING);
   gen.field("storeFsUUID",                  "",       CFGTYPE_STRING);
   gen.field("storeAllowFirstRunInit",       "true",   CFGTYPE_BOOL);
   gen.field("storeUseExtendedAttribs",      "true",   CFGTYPE_BOOL);
   gen.field("storeSelfHealEmptyFiles",      "true",   CFGTYPE_BOOL);
   gen.field("storeClientXAttrs",            "false",  CFGTYPE_BOOL);
   gen.field("storeClientACLs",              "false",  CFGTYPE_BOOL);
   gen.field("storeNFSv4ACLs",               "false",  CFGTYPE_BOOL);
   gen.field("sysTargetAttachmentFile",      "",       CFGTYPE_STRING); //  used by randominternode target chooser
   gen.field("tuneNumStreamListeners",       "1",      CFGTYPE_UINT);
   gen.field("tuneNumWorkers",               "0",      CFGTYPE_UINT); //  0 means automatic
   gen.field("tuneWorkerBufSize",            "1m",     CFGTYPE_INT64, CFGFLAG_HUMAN_READABLE_SIZE);
   gen.field("tuneNumCommSlaves",            "0",      CFGTYPE_UINT); //  0 means automatic
   gen.field("tuneCommSlaveBufSize",         "1m",     CFGTYPE_INT64, CFGFLAG_HUMAN_READABLE_SIZE);
   gen.field("tuneDefaultChunkSize",         "512k",   CFGTYPE_INT64, CFGFLAG_HUMAN_READABLE_SIZE);
   gen.field("tuneDefaultNumStripeTargets",  "4",      CFGTYPE_UINT);
   gen.field("tuneProcessFDLimit",           "50000",  CFGTYPE_UINT); //  0 means "don't touch limit"
   gen.field("tuneWorkerNumaAffinity",       "false",  CFGTYPE_BOOL);
   gen.field("tuneListenerNumaAffinity",     "false",  CFGTYPE_BOOL);
   gen.field("tuneBindToNumaZone",           "",       CFGTYPE_INT, CFGFLAG_EMPTY_IS_MINUS1); //  bind all threads to this zone, -1 means no binding
   gen.field("tuneListenerPrioShift",        "-1",     CFGTYPE_INT); //  inc/dec thread priority of listener components
   gen.field("tuneDirMetadataCacheLimit",    "1024",   CFGTYPE_INT64, CFGFLAG_HUMAN_READABLE_SIZE);
   gen.field("tuneTargetChooser",            "randomized" /*TARGETCHOOSERTYPE_RANDOMIZED_STR*/, CFGTYPE_STRING);
   gen.field("tuneLockGrantWaitMS",          "333",    CFGTYPE_UINT); //  time to wait for an ack per retry
   gen.field("tuneLockGrantNumRetries",      "15",     CFGTYPE_UINT); //  number of lock grant send retries until ack recv
   gen.field("tuneRotateMirrorTargets",      "false",  CFGTYPE_BOOL); //  true to use rotated targets list as mirrors
   gen.field("tuneEarlyUnlinkResponse",      "true",   CFGTYPE_BOOL); //  true to send response before chunk files unlink
   gen.field("tuneUsePerUserMsgQueues",      "false",  CFGTYPE_BOOL); //  true to use UserWorkContainer for MultiWorkQueue
   gen.field("tuneUseAggressiveStreamPoll",  "false",  CFGTYPE_BOOL); //  true to not sleep on epoll in streamlisv2
   gen.field("tuneNumResyncSlaves",          "12",     CFGTYPE_UINT);
   gen.field("tuneMirrorTimestamps",         "true",   CFGTYPE_BOOL);
   gen.field("tuneDisposalGCPeriod",         "0",      CFGTYPE_UINT); //  sleep between disposal garbage collector runs [seconds] = disabled
   gen.field("tuneChunkBalanceQueueLimit",       "100000", CFGTYPE_UINT);
   gen.field("tuneChunkBalanceLockingTimeLimit", "21600",  CFGTYPE_UINT);
   gen.field("quotaEarlyChownResponse",      "true",   CFGTYPE_BOOL); //  true to send response before chunk files chown
   gen.field("quotaEnableEnforcement",       "false",  CFGTYPE_BOOL);
   gen.field("sysTargetOfflineTimeoutSecs",  "180",    CFGTYPE_UINT);
   gen.field("sysAllowUserSetPattern",       "false",  CFGTYPE_BOOL);
   gen.field("sysFileEventLogTarget",        "",       CFGTYPE_STRING);
   gen.field("sysFileEventPersistDirectory", "",       CFGTYPE_STRING);
   gen.field("sysFileEventPersistSize",      "0",      CFGTYPE_INT64, CFGFLAG_HUMAN_READABLE_SIZE);
   gen.field("runDaemonized",                "false",  CFGTYPE_BOOL);
   gen.field("limitXAttrListLength",         "false",  CFGTYPE_BOOL);
   gen.field("pidFile",                      "",       CFGTYPE_STRING);
   gen.field("tuneResyncQueueLimit",         "250000", CFGTYPE_UINT);
   gen.field("sysRemoteInvalEnabled",        "false",  CFGTYPE_BOOL);
   gen.field("tuneInvalWatchMaxObjects",     "50000000",   CFGTYPE_UINT64);
   gen.field("tuneInvalWatchQueueSize",      "4m",     CFGTYPE_UINT64, CFGFLAG_HUMAN_READABLE_SIZE); // per-client invalidation queue cap; raises overflow threshold

   const char *dirpath = "../generated";  // relative to build directory
   const char *classname = "MetaConfigFields";

   generate_config_sources(&gen, dirpath, classname);
   return 0;
}
