package org.ruangfafa;

import org.ruangfafa.Service.DatabaseService;

import java.sql.Connection;

import static org.ruangfafa.Controller.ApplicationController.assignSellersUrls;

public class TestPart3 {
    private static final Connection DB = DatabaseService.getConnection();
    public static void main(String[] args) {
        assignSellersUrls(DB);

    }
}
