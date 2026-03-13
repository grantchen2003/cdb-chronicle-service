package io.github.grantchen2003.cdb.chronicle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EnvConfigTest {

    @Test
    void testGet_returnsValueWhenEnvVarIsSet() {
        final String value = EnvConfig.get("PATH");
        Assertions.assertNotNull(value);
        Assertions.assertFalse(value.isBlank());
    }

    @Test
    void testGet_throwsWhenEnvVarIsMissing() {
        final IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                EnvConfig.get("CHRONICLE_DEFINITELY_NOT_A_REAL_ENV_VAR")
        );

        assertTrue(ex.getMessage().contains("CHRONICLE_DEFINITELY_NOT_A_REAL_ENV_VAR"));
    }
}