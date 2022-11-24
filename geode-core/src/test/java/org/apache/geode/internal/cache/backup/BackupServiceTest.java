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
package org.apache.geode.internal.cache.backup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.util.Arrays;
import java.util.HashSet;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.DiskStoreStats;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PersistentOplogSet;
import org.apache.geode.internal.cache.persistence.PersistentMemberPattern;

public class BackupServiceTest {

  private BackupService backupService;

  private DistributionManager distributionManager;

  private final InternalDistributedMember sender = new InternalDistributedMember("localhost", 5555);

  private InternalCache cache;

  @Before
  public void setUp() throws Exception {
    cache = mock(InternalCache.class);
    distributionManager = mock(DistributionManager.class);
    InternalDistributedSystem distributedSystem = mock(InternalDistributedSystem.class);
    InternalDistributedMember distributedMember = mock(InternalDistributedMember.class);

    when(cache.getDistributionManager()).thenReturn(distributionManager);
    when(distributedSystem.getDistributedMember()).thenReturn(distributedMember);
    when(cache.getInternalDistributedSystem()).thenReturn(distributedSystem);
    when(distributedMember.toString()).thenReturn("member");
    when(distributionManager.addAllMembershipListenerAndGetAllIds(any()))
        .thenReturn(new HashSet<>(Arrays.asList(sender)));

    backupService = new BackupService(cache);
  }

  @Test
  public void throwsExceptionWhenBackupRequesterHasLeftDistributedSystem() {
    InternalDistributedMember oldSender = new InternalDistributedMember("localhost", 5556);
    assertThatThrownBy(() -> backupService.validateRequestingSender(oldSender))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void startBackupThrowsExceptionWhenAnotherBackupInProgress() throws Exception {
    BackupTask backupTask = mock(BackupTask.class);
    backupService.setCurrentTask(backupTask);
    assertThatThrownBy(() -> backupService.prepareBackup(sender, null))
        .isInstanceOf(IOException.class);
  }

  @Test
  public void doBackupThrowsExceptionWhenNoBackupInProgress() throws Exception {
    assertThatThrownBy(() -> backupService.doBackup()).isInstanceOf(IOException.class);
  }

  @Test
  public void prepareBackupReturnsEmptyPersistentIdsWhenBackupNotInProgress() throws Exception {
    assertThat(backupService.prepareBackup(sender, null).size()).isEqualTo(0);
  }

  @Test
  public void testBackupThrowsException() throws Exception {
    DiskStoreImpl diskStore = mock(DiskStoreImpl.class);
    when(diskStore.hasPersistedData()).thenReturn(true);
    when(diskStore.getPersistentID()).thenReturn(new PersistentMemberPattern(null, null, null, 0));
    when(cache.listDiskStoresIncludingRegionOwned())
        .thenReturn(new HashSet<>(Arrays.asList(diskStore)));
    DiskStoreStats status = mock(DiskStoreStats.class);
    BackupWriter writer = mock(BackupWriter.class);
    when(diskStore.getStats()).thenReturn(status);
    when(diskStore.getPersistentOplogSet()).thenReturn(mock(PersistentOplogSet.class));
    doNothing().when(status).startBackup();

    doThrow(new AccessDeniedException("")).when(writer).backupFiles(any());
    assertThat(backupService.prepareBackup(sender, writer).size()).isEqualTo(1);
    HashSet<DiskStoreBackupResult> persistentIDs = backupService.doBackup();
    assertThat(persistentIDs.size()).isEqualTo(1);
    assertThat(persistentIDs.iterator().next().getFailedReason())
        .isEqualTo(BackupFailedReason.NO_PERMISSION);

    doThrow(new IOException("There is not enough space on the disk")).when(writer)
        .backupFiles(any());
    assertThat(backupService.prepareBackup(sender, writer).size()).isEqualTo(1);
    persistentIDs = backupService.doBackup();
    assertThat(persistentIDs.size()).isEqualTo(1);
    assertThat(persistentIDs.iterator().next().getFailedReason())
        .isEqualTo(BackupFailedReason.NO_SPACE_LEFT);

    doThrow(new IOException("other reason")).when(writer).backupFiles(any());
    assertThat(backupService.prepareBackup(sender, writer).size()).isEqualTo(1);
    persistentIDs = backupService.doBackup();
    assertThat(persistentIDs.size()).isEqualTo(1);
    assertThat(persistentIDs.iterator().next().getFailedReason())
        .isEqualTo(BackupFailedReason.OTHER_DISK_REASON);

    doThrow(new RuntimeException("unknown reason")).when(writer).backupFiles(any());
    assertThat(backupService.prepareBackup(sender, writer).size()).isEqualTo(1);
    persistentIDs = backupService.doBackup();
    assertThat(persistentIDs.size()).isEqualTo(0);
  }
}
