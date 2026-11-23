package cl.duoc.ventas;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;

/**
 * Validación de un evento de venta antes de agregarlo.
 *
 * <p>Reglas:
 * <ul>
 *   <li>JSON parseable y objeto.</li>
 *   <li>Campos obligatorios presentes y no vacíos.</li>
 *   <li>{@code eventTime} en ISO-8601.</li>
 *   <li>{@code quantity} y {@code unitPriceClp} enteros positivos.</li>
 *   <li>{@code totalClp == quantity * unitPriceClp}.</li>
 * </ul>
 *
 * <p>Un evento inválido nunca se agrega: se deriva a la DLQ con un motivo.
 */
public final class SaleValidator {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final List<String> REQUIRED_TEXT_FIELDS = List.of(
            "eventId",
            "eventTime",
            "store",
            "storeFormat",
            "city",
            "channel",
            "category",
            "product",
            "customerId",
            "paymentMethod");

    private SaleValidator() {
    }

    public static ValidationResult validate(String json) {
        if (json == null || json.isBlank()) {
            return ValidationResult.invalid("JSON_VACIO", null, json);
        }

        JsonNode node;
        try {
            node = MAPPER.readTree(json);
        } catch (Exception e) {
            return ValidationResult.invalid("JSON_INVALIDO", null, json);
        }
        if (node == null || !node.isObject()) {
            return ValidationResult.invalid("JSON_INVALIDO", null, json);
        }

        for (String field : REQUIRED_TEXT_FIELDS) {
            if (text(node, field) == null) {
                return ValidationResult.invalid(
                        "CAMPO_OBLIGATORIO:" + field, text(node, "eventId"), json);
            }
        }

        String eventId = text(node, "eventId");

        try {
            Instant.parse(text(node, "eventTime"));
        } catch (Exception e) {
            return ValidationResult.invalid("EVENT_TIME_INVALIDO", eventId, json);
        }

        Integer quantity = integer(node, "quantity");
        if (node.get("quantity") == null || node.get("quantity").isNull()) {
            return ValidationResult.invalid("CAMPO_OBLIGATORIO:quantity", eventId, json);
        }
        if (quantity == null || quantity <= 0) {
            return ValidationResult.invalid("VALOR_FUERA_DE_RANGO:quantity", eventId, json);
        }

        Integer unitPrice = integer(node, "unitPriceClp");
        if (node.get("unitPriceClp") == null || node.get("unitPriceClp").isNull()) {
            return ValidationResult.invalid("CAMPO_OBLIGATORIO:unitPriceClp", eventId, json);
        }
        if (unitPrice == null || unitPrice <= 0) {
            return ValidationResult.invalid("VALOR_FUERA_DE_RANGO:unitPriceClp", eventId, json);
        }

        Integer total = integer(node, "totalClp");
        if (node.get("totalClp") == null || node.get("totalClp").isNull()) {
            return ValidationResult.invalid("CAMPO_OBLIGATORIO:totalClp", eventId, json);
        }
        if (total == null || total <= 0) {
            return ValidationResult.invalid("VALOR_FUERA_DE_RANGO:totalClp", eventId, json);
        }

        if (total != quantity * unitPrice) {
            return ValidationResult.invalid("TOTAL_INCONSISTENTE", eventId, json);
        }

        try {
            return ValidationResult.ok(MAPPER.treeToValue(node, Sale.class), json);
        } catch (Exception e) {
            return ValidationResult.invalid("JSON_INVALIDO", eventId, json);
        }
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        String text = value.asText();
        return text == null || text.isBlank() ? null : text;
    }

    private static Integer integer(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isIntegralNumber() || !value.canConvertToInt()) {
            return null;
        }
        return value.intValue();
    }
}
