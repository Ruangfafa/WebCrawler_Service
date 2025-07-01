package org.ruangfafa;

import org.ruangfafa.Service.DatabaseService;
import org.ruangfafa.Service.Logger;

import java.sql.Connection;

import static org.ruangfafa.Controller.ApplicationController.*;
import static org.ruangfafa.Service.DatabaseService.getState;

public class VerticalTest_tm {
    private static final Connection DB = DatabaseService.getConnection();
    private static int serverState;
    public static void main(String[] args) {
            //assignTargetSellersUrls(DB);
            //assignSellersUrls(DB);
            //assignSellerCategoryUrls(DB);

        }

}
