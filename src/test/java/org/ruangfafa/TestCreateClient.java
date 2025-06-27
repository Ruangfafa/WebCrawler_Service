package org.ruangfafa;

import org.ruangfafa.Service.DatabaseService;

import java.sql.Connection;

public class TestCreateClient {
    private static final Connection DB = DatabaseService.getConnection();
    public static void main(String[] args) {
        if (1==0) {
            System.out.println(DatabaseService.createClient(DB));
        }else {
            DatabaseService.deleteClient(DB, 1750923630103L);
            DatabaseService.deleteClient(DB, 1750923680055L);
        }
    }
}
