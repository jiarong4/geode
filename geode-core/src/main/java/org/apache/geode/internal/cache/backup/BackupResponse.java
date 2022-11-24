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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.remote.AdminResponse;
import org.apache.geode.internal.cache.persistence.PersistentMemberPattern;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.StaticSerialization;

/**
 * The response to a {@link PrepareBackupRequest}, {@link AbortBackupRequest}, or
 * {@link FinishBackupRequest}.
 */
public class BackupResponse extends AdminResponse {

  private HashSet<DiskStoreBackupResult> persistentIds;

  public BackupResponse() {
    super();
  }

  BackupResponse(InternalDistributedMember sender, HashSet<DiskStoreBackupResult> persistentIds) {
    setRecipient(sender);
    this.persistentIds = persistentIds;
  }

  Set<DiskStoreBackupResult> getPersistentIds() {
    return persistentIds;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    if (StaticSerialization.getVersionForDataStream(in).isOlderThan(KnownVersion.GEODE_1_15_0)) {
      persistentIds = new HashSet<>();
      HashSet<PersistentID> legacyResult = DataSerializer.readHashSet(in);
      if (legacyResult != null) {
        for (PersistentID persistentID : legacyResult) {
          persistentIds
              .add(new DiskStoreBackupResult(persistentID, BackupFailedReason.NONE));
        }
      }
    } else {
      persistentIds = DataSerializer.readHashSet(in);
    }
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    if (StaticSerialization.getVersionForDataStream(out).isOlderThan(KnownVersion.GEODE_1_15_0)) {
      HashSet<PersistentID> legacyData = new HashSet<>();
      if (persistentIds != null) {
        for (DiskStoreBackupResult diskStore : persistentIds) {
          if (BackupFailedReason.NONE == diskStore.getFailedReason()) {
            legacyData.add(new PersistentMemberPattern(diskStore.getPersistentID().getHost(),
                diskStore.getPersistentID().getDirectory(), diskStore.getPersistentID().getUUID(),
                0));
          }
        }
      }
      DataSerializer.writeHashSet(legacyData, out);
    } else {
      DataSerializer.writeHashSet(persistentIds, out);
    }
  }

  @Override
  public int getDSFID() {
    return BACKUP_RESPONSE;
  }

  @Override
  public String toString() {
    return getClass().getName() + ": " + persistentIds;
  }
}
