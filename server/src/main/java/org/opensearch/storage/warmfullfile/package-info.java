/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
/**
 * Warm-tier whole-file promotion: settings, state, and the node-level manager that serves eligible
 * files from a single local memory mapping instead of 8 MiB blocks.
 */
package org.opensearch.storage.warmfullfile;
