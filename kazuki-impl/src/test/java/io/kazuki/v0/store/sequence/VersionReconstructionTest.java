/**
 * Copyright 2014 Sunny Gleason
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kazuki.v0.store.sequence;

import io.kazuki.v0.internal.helper.TestSupport;
import io.kazuki.v0.store.Version;
import org.testng.annotations.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.*;

public class VersionReconstructionTest
    extends TestSupport
{
  @Test
  public void testDemo() throws Exception {
    final Version version = VersionImpl.createInternal(KeyImpl.createInternal("type", 1L), 1L);

    // Make the roundrip: Version - String - Version
    final String clientPayload = version.getIdentifier();
    final Version reconstructedVersion = VersionImpl.valueOf(clientPayload);

    // assure all is fine
    assertThat(reconstructedVersion, notNullValue());
    assertThat(reconstructedVersion, equalTo(version));
  }
}
