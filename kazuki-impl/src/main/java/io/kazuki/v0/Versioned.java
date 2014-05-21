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
package io.kazuki.v0;

/**
 * Interface that those components that are explicitly versioned will implement.
 * Intention is to allow both plug-in components (custom extensions) and applications and
 * frameworks that use Kazuki to detect exact version of Kazuki is in use.
 * This may be useful for example for ensuring that proper Jackson version is deployed
 * (beyond mechanisms that deployment system may have), as well as for possible
 * workarounds.
 */
public interface Versioned {
    /**
     * Method called to detect version of the component that implements this interface;
     * returned version should never be null, but may return specific "not available"
     * instance (see {@link Version} for details).
     */
    Version version();
}
