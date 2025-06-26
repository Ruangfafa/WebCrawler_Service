package org.ruangfafa;

import org.ruangfafa.Service.DatabaseService;

import java.sql.Connection;

import static org.ruangfafa.Service.DatabaseService.createClient;
import static org.ruangfafa.Service.DatabaseService.deleteClient;

public class Application {
    private static final Connection DB = DatabaseService.getConnection();

    public static void main(String[] args) {


    }
}
