/**
 * RelayMesh source tree root.
 *
 * <p>Primary entry points while reading code:
 *
 * <ul>
 *   <li>{@code io.relaymesh.Main} bootstraps the CLI process.</li>
 *   <li>{@code io.relaymesh.cli.RelayMeshCommand} maps commands to runtime APIs.</li>
 *   <li>{@code io.relaymesh.runtime.RelayMeshRuntime} orchestrates submission, dispatch, retries, and recovery.</li>
 *   <li>{@code io.relaymesh.storage.TaskStore} is the authoritative persistence layer.</li>
 * </ul>
 */
package io.relaymesh;