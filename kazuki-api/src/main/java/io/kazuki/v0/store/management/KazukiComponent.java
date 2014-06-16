/**
 * Copyright 2014 Sunny Gleason and original author or authors
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
package io.kazuki.v0.store.management;

/**
 * Registration interface for the "component side". A service implements this class because it wants
 * to receive registration callbacks from Kazuki components.
 */
public interface KazukiComponent<T> {
  /**
   * Returns the component descriptor for this component.
   * 
   * @return ComponentDescriptor instance describing the component
   */
  ComponentDescriptor<T> getComponentDescriptor();

  /**
   * Registration interface for the "component side". A component implements this method with
   * injection to receive an instance of the ComponentRegistrar. In the body of the implementation
   * method, the component should call {@link ComponentRegistrar#register(ComponentDescriptor)}.
   */
  void registerAsComponent(ComponentRegistrar manager);
}
