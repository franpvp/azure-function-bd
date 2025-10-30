package org.example.functions;

import com.azure.core.credential.AzureKeyCredential;
import com.azure.core.util.BinaryData;
import com.azure.messaging.eventgrid.EventGridEvent;
import com.azure.messaging.eventgrid.EventGridPublisherClient;
import com.azure.messaging.eventgrid.EventGridPublisherClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.*;
import org.example.functions.dto.TradeDto;

import java.sql.*;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;

public class TradeFunction {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    @FunctionName("crearTrade")
    public HttpResponseMessage crearTrade(
            @HttpTrigger(
                    name = "req",
                    methods = { HttpMethod.POST },
                    authLevel = AuthorizationLevel.ANONYMOUS,
                    route = "trades"
            ) HttpRequestMessage<Optional<String>> request,
            final ExecutionContext ctx) {

        try {
            String body = request.getBody().orElse("");
            if (body.isBlank()) {
                return badJson(request, "El body no puede ser vacío");
            }

            TradeDto trade = MAPPER.readValue(body, TradeDto.class);

            if (trade.getMonto() == null || trade.getMonto() < 0) {
                return badJson(request, "monto es obligatorio y debe ser >= 0");
            }
            if (trade.getFechaCreacion() == null) {
                trade.setFechaCreacion(LocalDate.now());
            }
            if (trade.getIdCliente() == null) {
                return badJson(request, "idCliente es obligatorio");
            }

            String pathWallet = "";
            String walletPath = Optional.ofNullable(System.getenv("ORACLE_WALLET_DIR"))
                    .filter(s -> !s.isBlank())
                    .orElse(pathWallet);

            String url  = "";
            String user = "";
            String pass = "";

            Long newId = insertarTrade(url, user, pass, trade, ctx);

            try {
                publicarTradeCreado(trade.builder().idTrade(newId).build(), ctx);
            } catch (Exception egx) {
                ctx.getLogger().warning("Trade creado pero falló publicar a Event Grid: " + egx.getMessage());
            }

            String json = MAPPER.writeValueAsString(Map.of(
                    "message", "Trade creado",
                    "idTrade", newId,
                    "payload", trade
            ));

            return request.createResponseBuilder(HttpStatus.OK) // <-- 200 OK
                    .header("Content-Type", "application/json")
                    .body(json) // String JSON ya serializado
                    .build();

        } catch (Exception ex) {
            ctx.getLogger().severe("Error en crearTrade: " + ex.getMessage());
            try {
                String err = MAPPER.writeValueAsString(Map.of("error", ex.getMessage()));
                return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                        .header("Content-Type", "application/json")
                        .body(err)
                        .build();
            } catch (Exception ignore) {
                // Fallback minimal si fallara Jackson
                return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                        .header("Content-Type", "application/json")
                        .body("{\"error\":\"" + safe(ex.getMessage()) + "\"}")
                        .build();
            }
        }
    }

    private static Long insertarTrade(String url, String user, String pass, TradeDto trade, ExecutionContext ctx) throws Exception {
        try (Connection conn = DriverManager.getConnection(url, user, pass)) {
            conn.setAutoCommit(false);

            validacionCreacionTablaTrades(conn, ctx);

            String sql = "INSERT INTO TRADE (MONTO, FECHA_CREACION, ID_CLIENTE) VALUES (?, ?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(sql, new String[] {"ID_TRADE"})) {
                ps.setBigDecimal(1, java.math.BigDecimal.valueOf(trade.getMonto()));
                ps.setDate(2, java.sql.Date.valueOf(trade.getFechaCreacion()));
                ps.setLong(3, trade.getIdCliente());

                int rows = ps.executeUpdate();
                if (rows == 0) {
                    conn.rollback();
                    throw new RuntimeException("No se insertó el trade");
                }

                Long newId = null;
                try (ResultSet gk = ps.getGeneratedKeys()) {
                    if (gk != null && gk.next()) {
                        Object val = gk.getObject(1);
                        if (val instanceof Number n) newId = n.longValue();
                    }
                }

                conn.commit();
                if (newId == null) throw new RuntimeException("No se obtuvo ID_TRADE autogenerado");
                return newId;
            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        }
    }

    private static void publicarTradeCreado(TradeDto trade, ExecutionContext ctx) {
        final String eventGridTopicEndpoint = "";
        final String eventGridTopicKey      = "";

        EventGridPublisherClient<EventGridEvent> client =
                new EventGridPublisherClientBuilder()
                        .endpoint(eventGridTopicEndpoint)
                        .credential(new AzureKeyCredential(eventGridTopicKey))
                        .buildEventGridEventPublisherClient();

        EventGridEvent event = new EventGridEvent(
                "/trades",
                "trade.created.v1",
                BinaryData.fromObject(trade),
                "1.0"
        );
        event.setEventTime(OffsetDateTime.now());
        client.sendEvent(event);
        ctx.getLogger().info("Event Grid: trade.created.v1 publicado para idTrade=" + trade.getIdTrade());
    }

    private HttpResponseMessage badJson(HttpRequestMessage<?> request, String mensaje) {
        try {
            String json = MAPPER.writeValueAsString(Map.of("error", mensaje));
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                    .header("Content-Type", "application/json")
                    .body(json) // String JSON
                    .build();
        } catch (Exception e) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                    .header("Content-Type", "application/json")
                    .body("{\"error\":\"" + safe(mensaje) + "\"}")
                    .build();
        }
    }

    private static String safe(String s) { return s == null ? "" : s.replace("\"","'"); }

    private static void validacionCreacionTablaTrades(Connection conn, ExecutionContext ctx) throws SQLException {
        String checkSql = """
            SELECT COUNT(*) 
            FROM all_tables 
            WHERE table_name = 'TRADE'
            """;

        try (PreparedStatement ps = conn.prepareStatement(checkSql);
             ResultSet rs = ps.executeQuery()) {

            boolean exists = rs.next() && rs.getInt(1) > 0;

            if (!exists) {
                ctx.getLogger().info("La tabla TRADE no existe, creando...");

                String ddl = """
                    CREATE TABLE TRADE (
                        ID_TRADE NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                        MONTO NUMBER(18,2) NOT NULL,
                        FECHA_CREACION DATE DEFAULT SYSDATE NOT NULL,
                        ID_CLIENTE NUMBER NOT NULL
                    )
                    """;

                try (Statement st = conn.createStatement()) {
                    st.executeUpdate(ddl);
                    ctx.getLogger().info("Tabla TRADE creada exitosamente ✅");
                }
            } else {
                ctx.getLogger().info("La tabla TRADE ya existe.");
            }
        }
    }
}