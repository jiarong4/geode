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

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.persistence.PersistentID;


/**
 * Backup result
 *
 */
public class DiskStoreBackupResult implements DataSerializable {

  private PersistentID persistentID;

  private BackupFailedReason failedReason;

  public DiskStoreBackupResult(PersistentID persistentID) {
    this(persistentID, BackupFailedReason.NONE);
  }

  public DiskStoreBackupResult(PersistentID persistentID, BackupFailedReason failedReason) {
    this.persistentID = persistentID;
    this.failedReason = failedReason;
  }

  public DiskStoreBackupResult() {
    // Used for deserialization only
  }

  public PersistentID getPersistentID() {
    return persistentID;
  }

  public BackupFailedReason getFailedReason() {
    return failedReason;
  }

  public void setFailedReason(BackupFailedReason failedReason) {
    this.failedReason = failedReason;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    persistentID = DataSerializer.readObject(in);
    try {
      failedReason = DataSerializer.readEnum(BackupFailedReason.class, in);
    } catch (Exception e) {
      failedReason = BackupFailedReason.UNKNOWN;
    }
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(persistentID, out);
    DataSerializer.writeEnum(failedReason, out);
  }

  @Override
  public int hashCode() {
    return persistentID.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    DiskStoreBackupResult other = (DiskStoreBackupResult) obj;
    return persistentID.equals(other.getPersistentID());
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(persistentID);
    result.append(failedReason);
    return result.toString();
  }
}
