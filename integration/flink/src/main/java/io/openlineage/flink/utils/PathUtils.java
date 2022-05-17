/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.flink.utils;

import java.net.URI;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PathUtils {

  private static final String DEFAULT_SCHEME = "file";

  public static DatasetIdentifier fromURI(URI location) {
    return PathUtils.fromURI(location, DEFAULT_SCHEME);
  }

  public static DatasetIdentifier fromURI(URI location, String defaultScheme) {
    if (location.isAbsolute() && location.getAuthority() == null && location.getScheme() == null) {
      return new DatasetIdentifier(location.toString(), defaultScheme);
    }
    String namespace =
        Optional.ofNullable(location.getAuthority())
            .map(a -> String.format("%s://%s", location.getScheme(), a))
            .orElseGet(() -> (location.getScheme() != null) ? location.getScheme() : defaultScheme);
    String name = removeFirstSlashIfSingleSlashInString(location.getPath());
    return new DatasetIdentifier(name, namespace);
  }

  private static String removeFirstSlashIfSingleSlashInString(String name) {
    if (name.chars().filter(x -> x == '/').count() == 1 && name.startsWith("/")) {
      return name.substring(1);
    }
    return name;
  }
}
