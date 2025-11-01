package org.example.functions.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TradeDto {
    private Long idTrade;
    private Long monto;
    private String canal;
    private LocalDate fechaCreacion;
    private Long idCliente;
}
