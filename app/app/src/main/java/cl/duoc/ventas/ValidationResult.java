package cl.duoc.ventas;

/**
 * Resultado de validar un evento crudo.
 * Si {@link #valid()} es true, {@link #sale()} trae el DTO listo para agregar.
 * Si es false, {@link #reason()} explica por qué se envía a la DLQ y
 * {@link #original()} conserva el valor original recibido.
 */
public record ValidationResult(Sale sale, String reason, String eventId, String original) {

    public boolean valid() {
        return sale != null;
    }

    public static ValidationResult ok(Sale sale, String original) {
        return new ValidationResult(sale, null, sale.eventId(), original);
    }

    public static ValidationResult invalid(String reason, String eventId, String original) {
        return new ValidationResult(null, reason, eventId, original);
    }
}
