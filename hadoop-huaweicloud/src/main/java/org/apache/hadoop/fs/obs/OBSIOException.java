/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.obs;

import java.io.IOException;

import com.obs.services.exception.ObsException;
import com.google.common.base.Preconditions;

/**
 * IOException equivalent of an {@link ObsException}.
 */
public class OBSIOException extends IOException {

  private static final long serialVersionUID = -1582681108285856259L;
  private final String operation;

  public OBSIOException(String operation,
                        ObsException cause) {
    super(cause);
    Preconditions.checkArgument(operation != null, "Null 'operation' argument");
    Preconditions.checkArgument(cause != null, "Null 'cause' argument");
    this.operation = operation;
  }

  public ObsException getCause() {
    return (ObsException) super.getCause();
  }

  @Override
  public String getMessage() {
    return operation + ": " + getCause().getErrorMessage();
  }

}
