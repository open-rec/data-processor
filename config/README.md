# Java formatter

`eclipse-java-alibaba-style.xml` is vendored from Alibaba's
[P3C Eclipse profile](https://github.com/alibaba/p3c/blob/master/p3c-formatter/eclipse-codestyle.xml)
(retrieved 2026-10-16). It uses four spaces, a 120-column wrapping target and
one blank line before methods. The profile is kept byte-for-byte identical to
the upstream file, without custom wrapping or blank-line overrides.

Spotless 3.10.3 and Eclipse JDT 4.37 are pinned in the parent POM. All modules'
`src/main/java` and `src/test/java` files are included; generated sources and
`target` are excluded. The `.mvn` marker makes the profile resolve from the
reactor root even when Maven is invoked inside a module.

Import this profile into Eclipse, or use IntelliJ's Eclipse Code Formatter
plugin with the same JDT version. Enable format-on-save in the IDE for automatic
editing feedback. Maven remains the authoritative formatter; `.editorconfig`
only supplies basic whitespace settings. This enforces formatting, not all
semantic rules in the Alibaba Java coding guidelines.
