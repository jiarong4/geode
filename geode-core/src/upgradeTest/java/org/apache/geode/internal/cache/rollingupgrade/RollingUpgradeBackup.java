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
package org.apache.geode.internal.cache.rollingupgrade;

import static java.util.stream.Collectors.toList;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.version.VmConfigurations.hasGeodeVersion;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Collection;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.backup.BackupOperation;
import org.apache.geode.management.BackupStatus;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.test.version.TestVersions;
import org.apache.geode.test.version.VersionManager;
import org.apache.geode.test.version.VmConfiguration;
import org.apache.geode.test.version.VmConfigurations;

public class RollingUpgradeBackup extends RollingUpgrade2DUnitTestBase {

  private File backupBaseDir;

  @Parameterized.Parameters(name = "From {0}")
  public static Collection<VmConfiguration> data() {
    TestVersion minimumGeodeVersion = TestVersion.valueOf("1.7.0");
    return VmConfigurations.upgrades().stream()
        .filter(hasGeodeVersion(TestVersions.atLeast(minimumGeodeVersion)))
        .collect(toList());
  }

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Test
  public void testBackup() throws Exception {
    backupBaseDir = temporaryFolder.newFolder("backupDir");
    final Host host = Host.getHost(0);
    VM oldServerAndLocator = host.getVM(sourceConfiguration, 0);
    VM oldServer2 = host.getVM(sourceConfiguration, 1);
    VM currentServer1 = host.getVM(VersionManager.CURRENT_VERSION, 2);
    VM currentServer2 = host.getVM(VersionManager.CURRENT_VERSION, 3);

    String hostName = NetworkUtils.getServerHostName();
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    try {
      Properties props = getSystemProperties();
      props.remove(DistributionConfig.LOCATORS_NAME);
      invokeRunnableInVMs(invokeStartLocatorAndServer(hostName, port, props),
          oldServerAndLocator);

      props.put(DistributionConfig.LOCATORS_NAME, hostName + "[" + port + "]");
      invokeRunnableInVMs(invokeCreateCache(props), currentServer1, currentServer2, oldServer2);
      invokeRunnableInVMs(invokeCreateRegion("backup-test", RegionShortcut.REPLICATE_PERSISTENT),
          currentServer1, currentServer2, oldServerAndLocator, oldServer2);
      oldServerAndLocator.invoke(invokeAndAssertBackup(backupBaseDir));
      VM newServerAndLocator =
          rollLocatorAndServerToCurrent(oldServerAndLocator, hostName, port, props);
      invokeRunnableInVMs(invokeCreateRegion("backup-test", RegionShortcut.REPLICATE_PERSISTENT),
          newServerAndLocator);
      newServerAndLocator.invoke(invokeAndAssertBackup(backupBaseDir));
    } finally {
      invokeRunnableInVMs(invokeCloseCache(), currentServer1, currentServer2, oldServer2,
          oldServerAndLocator);
    }
  }

  CacheSerializableRunnable invokeAndAssertBackup(final File file) {
    return new CacheSerializableRunnable("execute: backup") {
      @Override
      public void run2() {
        try {
          InternalCache internalCache = (InternalCache) cache;
          BackupStatus status =
              new BackupOperation(internalCache.getDistributionManager(), internalCache)
                  .backupAllMembers(file.toString(), null);
          assertThat(status.getBackedUpDiskStores()).hasSize(4);
        } catch (Exception e) {
          fail("Error backup", e);
        }
      }
    };
  }

  VM rollLocatorAndServerToCurrent(VM rollLocator, final String serverHostName, final int port,
      final Properties props) {
    // Roll the locator
    rollLocator.invoke(invokeCloseCache());
    VM newLocator = Host.getHost(0).getVM(VersionManager.CURRENT_VERSION, rollLocator.getId());
    newLocator.invoke(invokeStartLocatorAndServer(serverHostName, port, props));
    return newLocator;
  }
}
