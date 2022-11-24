/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.management.internal.cli.commands;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.backup.BackupFailedReason;
import org.apache.geode.internal.cache.backup.BackupOperation;
import org.apache.geode.internal.cache.backup.DiskStoreBackupResult;
import org.apache.geode.management.BackupStatus;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class BackupDiskStoreCommand extends GfshCommand {

  public static final String BACKED_UP_DISKSTORES_SECTION = "backed-up-diskstores";
  public static final String FAILED_BACKUP_DISKSTORES_SECTION = "failed-backup-diskstores";
  public static final String OFFLINE_DISKSTORES_SECTION = "offline-diskstores";

  /**
   * Internally, we also verify the resource operation permissions CLUSTER:WRITE:DISK if the region
   * is persistent
   */
  @CliCommand(value = CliStrings.BACKUP_DISK_STORE, help = CliStrings.BACKUP_DISK_STORE__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DISKSTORE})
  @ResourceOperation(resource = ResourcePermission.Resource.DATA,
      operation = ResourcePermission.Operation.READ)
  public ResultModel backupDiskStore(
      @CliOption(key = CliStrings.BACKUP_DISK_STORE__DISKDIRS,
          help = CliStrings.BACKUP_DISK_STORE__DISKDIRS__HELP, mandatory = true) String targetDir,
      @CliOption(key = CliStrings.BACKUP_DISK_STORE__BASELINEDIR,
          help = CliStrings.BACKUP_DISK_STORE__BASELINEDIR__HELP) String baselineDir,
      @CliOption(key = CliStrings.BACKUP_INCLUDE_DISK_STORES,
          help = CliStrings.BACKUP_INCLUDE_DISK_STORES__HELP) String[] includeDiskStores) {

    authorize(ResourcePermission.Resource.CLUSTER, ResourcePermission.Operation.WRITE,
        ResourcePermission.Target.DISK);

    ResultModel result = new ResultModel();
    try {
      InternalCache cache = (InternalCache) getCache();
      DistributionManager dm = cache.getDistributionManager();
      BackupStatus backupStatus;

      String includeDiskStoresString = null;
      if (includeDiskStores != null && includeDiskStores.length > 0) {
        for (String name : includeDiskStores) {
          if (name != null && !name.isEmpty()) {
            if (!diskStoreExists(name)) {
              return ResultModel.createError(CliStrings.format(
                  CliStrings.BACKUP_DISK_STORE__MSG__SPECIFY_VALID_INCLUDE_DISKSTORE_UNKNOWN_DISKSTORE_0,
                  new Object[] {name}));
            }
          } else {
            return ResultModel.createError(CliStrings.format(
                CliStrings.BACKUP_DISK_STORE__MSG__SPECIFY_VALID_INCLUDE_DISKSTORE_UNKNOWN_DISKSTORE_1));
          }
        }
        includeDiskStoresString = StringUtils.join(includeDiskStores, ",");
      }

      if (baselineDir != null && !baselineDir.isEmpty()) {
        backupStatus =
            new BackupOperation(dm, dm.getCache()).backupAllMembers(targetDir, baselineDir,
                includeDiskStoresString);
      } else {
        backupStatus = new BackupOperation(dm, dm.getCache()).backupAllMembers(targetDir, null,
            includeDiskStoresString);
      }

      Map<DistributedMember, Set<DiskStoreBackupResult>> backedupMemberDiskstoreMap =
          backupStatus.getBackedUpDiskStores();

      if (backupResultExists(backedupMemberDiskstoreMap, true)) {
        TabularResultModel backedupDiskStoresTable = result.addTable(BACKED_UP_DISKSTORES_SECTION);
        backedupDiskStoresTable
            .setHeader(CliStrings.BACKUP_DISK_STORE_MSG_BACKED_UP_DISK_STORES);
        writeBackupDiskStoreResult(backedupDiskStoresTable, backedupMemberDiskstoreMap, true);
      } else {
        result.addInfo().addLine(CliStrings.BACKUP_DISK_STORE_MSG_NO_DISKSTORES_BACKED_UP);
      }

      if (backupResultExists(backedupMemberDiskstoreMap, false)) {
        TabularResultModel backedupDiskStoresTable =
            result.addTable(FAILED_BACKUP_DISKSTORES_SECTION);
        backedupDiskStoresTable
            .setHeader(CliStrings.BACKUP_DISK_STORE_MSG_BACKUP_FAILED_DISK_STORES);
        writeBackupDiskStoreResult(backedupDiskStoresTable, backedupMemberDiskstoreMap, false);
      }

      Set<PersistentID> offlineDiskStores = backupStatus.getOfflineDiskStores();

      if (!offlineDiskStores.isEmpty()) {
        TabularResultModel offlineDiskStoresTable = result.addTable(OFFLINE_DISKSTORES_SECTION);
        offlineDiskStoresTable.setHeader(CliStrings.BACKUP_DISK_STORE_MSG_OFFLINE_DISK_STORES);

        for (PersistentID offlineDiskStore : offlineDiskStores) {
          offlineDiskStoresTable.accumulate(CliStrings.BACKUP_DISK_STORE_MSG_UUID,
              offlineDiskStore.getUUID().toString());
          offlineDiskStoresTable.accumulate(CliStrings.BACKUP_DISK_STORE_MSG_HOST,
              offlineDiskStore.getHost().getHostName());
          offlineDiskStoresTable.accumulate(CliStrings.BACKUP_DISK_STORE_MSG_DIRECTORY,
              offlineDiskStore.getDirectory());
        }
      }
    } catch (Exception e) {
      return ResultModel.createError(e.getMessage());
    }
    return result;
  }

  private void writeBackupDiskStoreResult(TabularResultModel table,
      Map<DistributedMember, Set<DiskStoreBackupResult>> diskStoreMap, boolean success) {
    Set<DistributedMember> backedupMembers = diskStoreMap.keySet();
    for (DistributedMember member : backedupMembers) {
      Set<DiskStoreBackupResult> backedupDiskStores = diskStoreMap.get(member);
      boolean printMember = true;
      String memberName = member.getName();

      if (memberName == null || memberName.isEmpty()) {
        memberName = member.getId();
      }
      for (DiskStoreBackupResult diskStore : backedupDiskStores) {
        if (success && diskStore.getFailedReason() != BackupFailedReason.NONE) {
          continue;
        }
        if (!success && diskStore.getFailedReason() == BackupFailedReason.NONE) {
          continue;
        }
        String UUID = diskStore.getPersistentID().getUUID().toString();
        String hostName = diskStore.getPersistentID().getHost().getHostName();
        String directory = diskStore.getPersistentID().getDirectory();
        BackupFailedReason failedReason = diskStore.getFailedReason();

        if (printMember) {
          writeToBackupDiskStoreTable(table, memberName, UUID, hostName,
              directory, failedReason);
          printMember = false;
        } else {
          writeToBackupDiskStoreTable(table, "", UUID, hostName, directory, failedReason);
        }
      }
    }
  }

  private void writeToBackupDiskStoreTable(TabularResultModel table, String memberId, String UUID,
      String host, String directory, BackupFailedReason failedReason) {
    table.accumulate(CliStrings.BACKUP_DISK_STORE_MSG_MEMBER, memberId);
    table.accumulate(CliStrings.BACKUP_DISK_STORE_MSG_UUID, UUID);
    table.accumulate(CliStrings.BACKUP_DISK_STORE_MSG_DIRECTORY, directory);
    table.accumulate(CliStrings.BACKUP_DISK_STORE_MSG_HOST, host);
    if (BackupFailedReason.NONE != failedReason) {
      table.accumulate(CliStrings.BACKUP_DISK_STORE_MSG_FAILED_REASON,
          getFailedReasonMessage(failedReason));
    }
  }

  private boolean diskStoreExists(String diskStoreName) {
    ManagementService managementService = getManagementService();
    DistributedSystemMXBean dsMXBean = managementService.getDistributedSystemMXBean();

    return Arrays.stream(dsMXBean.listMembers()).anyMatch(
        member -> DiskStoreCommandsUtils.diskStoreBeanAndMemberBeanDiskStoreExists(dsMXBean, member,
            diskStoreName));
  }

  private boolean backupResultExists(
      Map<DistributedMember, Set<DiskStoreBackupResult>> diskStoreMap,
      boolean success) {
    if (diskStoreMap.isEmpty()) {
      return false;
    }
    for (Set<DiskStoreBackupResult> backedupDiskStores : diskStoreMap.values()) {
      for (DiskStoreBackupResult diskStore : backedupDiskStores) {
        if (success && diskStore.getFailedReason() == BackupFailedReason.NONE) {
          return true;
        }
        if (!success && diskStore.getFailedReason() != BackupFailedReason.NONE) {
          return true;
        }
      }
    }
    return false;
  }

  private String getFailedReasonMessage(BackupFailedReason failedReason) {
    switch (failedReason) {
      case NO_PERMISSION:
        return CliStrings.BACKUP_DISK_STORE_MSG_FAILED_REASON_NO_PERMISSION;
      case NO_SPACE_LEFT:
        return CliStrings.BACKUP_DISK_STORE_MSG_FAILED_REASON_NO_SPACE;
      case OTHER_DISK_REASON:
        return CliStrings.BACKUP_DISK_STORE_MSG_FAILED_REASON_OTHER_DISK_REASON;
      default:
        return CliStrings.BACKUP_DISK_STORE_MSG_FAILED_REASON_UNKNOWN;
    }
  }
}
