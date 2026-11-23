package cl.duoc.ventas;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SaleValidatorTest {

    private static final String VALID = TestSupport.saleJson(
            "evt-1", "2026-01-01T00:00:10Z", "Tienda A", 2, 1000);

    @Test
    void aceptaEventoValido() {
        ValidationResult result = SaleValidator.validate(VALID);

        assertTrue(result.valid());
        assertEquals("evt-1", result.sale().eventId());
        assertEquals(2000, result.sale().totalClp());
        assertEquals(VALID, result.original());
    }

    @Test
    void rechazaJsonMalformado() {
        ValidationResult result = SaleValidator.validate("{\"eventId\":");

        assertFalse(result.valid());
        assertEquals("JSON_INVALIDO", result.reason());
    }

    @Test
    void rechazaCampoObligatorioFaltante() {
        ValidationResult result = SaleValidator.validate(
                TestSupport.withoutField(VALID, "quantity"));

        assertFalse(result.valid());
        assertEquals("CAMPO_OBLIGATORIO:quantity", result.reason());
        assertEquals("evt-1", result.eventId());
    }

    @Test
    void rechazaTotalInconsistente() {
        ValidationResult result = SaleValidator.validate(
                TestSupport.withInt(VALID, "totalClp", 1999));

        assertFalse(result.valid());
        assertEquals("TOTAL_INCONSISTENTE", result.reason());
    }

    @Test
    void rechazaEventTimeInvalido() {
        ValidationResult result = SaleValidator.validate(
                TestSupport.withText(VALID, "eventTime", "ayer"));

        assertFalse(result.valid());
        assertEquals("EVENT_TIME_INVALIDO", result.reason());
    }

    @Test
    void rechazaCantidadNoPositiva() {
        ValidationResult result = SaleValidator.validate(
                TestSupport.withInt(VALID, "quantity", 0));

        assertFalse(result.valid());
        assertEquals("VALOR_FUERA_DE_RANGO:quantity", result.reason());
    }
}
