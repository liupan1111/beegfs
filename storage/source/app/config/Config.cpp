#include <common/toolkit/StringTk.h>
#include <common/toolkit/UnitTk.h>
#include "Config.h"

#include <../generated/StorageConfigFields.inc>  // bake in data definitions

#define CONFIG_DEFAULT_CFGFILENAME "/etc/beegfs/beegfs-storage.conf"


Config::Config(int argc, char** argv) :
      AbstractConfig(argc, argv)
{
   initConfig(argc, argv, true, false);
}

/**
 * Sets the default values for each configurable in the configMap.
 *
 * @param addDashes currently unused
 */
void Config::loadDefaults(bool addDashes)
{
   AbstractConfig::loadDefaults(addDashes);

   commonLoadConfigDefaults(&configMap, ArraySlice(StorageConfigFields_infos), addDashes);
}

/**
 * @param addDashes currently usused
 */
void Config::applyConfigMap(bool enableException, bool addDashes)
{
   AbstractConfig::applyConfigMap(false, addDashes);

   commonApplyConfigMap(static_cast<StorageConfigFields *>(this), &this->configMap, ArraySlice(StorageConfigFields_infos).asConst(), enableException, addDashes);

   {
      std::vector<std::string> cleaned;
      for (auto const& item : this->storeStorageDirectory)
         if (!item.empty())
            cleaned.push_back(item);
      this->storeStorageDirectory = std::move(cleaned);
   }
   this->storageDirectories.clear();
   for (auto const& item : this->storeStorageDirectory)
      this->storageDirectories.push_back(Path(item));

   {
      std::vector<std::string> cleaned;
      for (auto const& item : this->storeFsUUID)
         if (!item.empty())
            cleaned.push_back(item);
      this->storeFsUUID = std::move(cleaned);
   }
   this->storeFsUUIDs.clear();
   for (auto const& item : this->storeFsUUID)
      this->storeFsUUIDs.push_back(item);

   if (this->sysTargetOfflineTimeoutSecs < 30)
   {
      throw InvalidConfigException("Invalid sysTargetOfflineTimeoutSecs value "
            + std::to_string(this->sysTargetOfflineTimeoutSecs) + " (must be at least 30)");
   }

   if(tuneAsyncIOBackend != "libaio" && tuneAsyncIOBackend != "io_uring")
      throw InvalidConfigException("Invalid tuneAsyncIOBackend value: " + tuneAsyncIOBackend);
}

void Config::initImplicitVals()
{
   // tuneFileReadAheadTriggerSize (should be ">= tuneFileReadAheadSize")
   if(tuneFileReadAheadTriggerSize < tuneFileReadAheadSize)
      tuneFileReadAheadTriggerSize = tuneFileReadAheadSize;

   // connInterfacesList(/File)
   AbstractConfig::initInterfacesList(connInterfacesFile, connInterfacesList);

   AbstractConfig::initSocketBufferSizes();

   // check if sync_file_range was enabled on a distro that doesn't support it
   #ifndef CONFIG_DISTRO_HAS_SYNC_FILE_RANGE
      if(tuneFileWriteSyncSize)
      {
         throw InvalidConfigException(
            "Config option is not supported for this distribution: 'tuneFileWriteSyncSize'");
      }
   #endif

   // connAuthHash
   AbstractConfig::initConnAuthHash(connAuthFile, &connAuthHash);
}
