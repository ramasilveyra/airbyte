/*
 * MIT License
 *
 * Copyright (c) 2020 Airbyte
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.airbyte.server.handlers;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.api.model.ConnectionIdRequestBody;
import io.airbyte.api.model.TransformationsCreate;
import io.airbyte.api.model.TransformationsRead;
import io.airbyte.api.model.TransformationsUpdate;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.UUID;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransformationsHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransformationsHandler.class);

  private final ConfigRepository configRepository;

  @VisibleForTesting
  TransformationsHandler(final ConfigRepository configRepository, final Supplier<UUID> uuidGenerator) {
    this.configRepository = configRepository;
  }

  public TransformationsHandler(final ConfigRepository configRepository) {
    this(configRepository, UUID::randomUUID);
  }

  public TransformationsRead createTransformations(TransformationsCreate transformationsCreate)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    // TODO chris
    return buildTransformationsRead(transformationsCreate.getConnectionId());
  }

  public TransformationsRead updateTransformations(TransformationsUpdate transformationsUpdate)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    // TODO chris
    return buildTransformationsRead(transformationsUpdate.getConnectionId());
  }

  public TransformationsRead getTransformationsForConnection(ConnectionIdRequestBody connectionIdRequestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    // TODO chris
    return buildTransformationsRead(connectionIdRequestBody.getConnectionId());
  }

  public void deleteTransformationsForConnection(ConnectionIdRequestBody connectionIdRequestBody)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final TransformationsRead TransformationsRead = getTransformationsForConnection(connectionIdRequestBody);
    // TODO chris
    deleteTransformations(TransformationsRead);
  }

  public void deleteTransformations(TransformationsRead transformationsRead) throws ConfigNotFoundException, IOException, JsonValidationException {
    final TransformationsUpdate transformationsUpdate = new TransformationsUpdate();
    // TODO chris
    updateTransformations(transformationsUpdate);
  }

  private TransformationsRead buildTransformationsRead(UUID connectionId)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    // TODO chris
    return new TransformationsRead();
  }

}
