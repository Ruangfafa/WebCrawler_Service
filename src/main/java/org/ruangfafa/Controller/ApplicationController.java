package org.ruangfafa.Controller;

import org.ruangfafa.Service.DatabaseService;
import org.ruangfafa.Service.Logger;
import org.ruangfafa.Service.UrlBuildService;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.ruangfafa.Service.DatabaseService.*;

public class ApplicationController {
    private static final String WORKDIC = "ApplicationController.java";

    public static void assignTargetSellersUrls(Connection conn) {
        if (!setState(conn, 0, 1)) {
            Logger.log("❌ 设置本机 serverDevice 占用状态失败，终止分配", WORKDIC);
            return;
        }
        try {
            List<String> targetSelldersUrls = pullTargetSellers(conn);
            List<Long> freeClients = pullClients(conn);
            if (targetSelldersUrls.isEmpty() || freeClients.isEmpty()) {
                Logger.log("⚠️ 没有可分配的 sellerUrl 或空闲 Client", WORKDIC);
                return;
            }
            int clientCount = freeClients.size();
            List<Long> usedClients = new ArrayList<>();
            for (int i = 0; i < targetSelldersUrls.size(); i++) {
                long device = freeClients.get(i % clientCount);
                assignUrlToClient(conn, device, targetSelldersUrls.get(i));
                if (!usedClients.contains(device)) {
                    usedClients.add(device);
                }
            }
            for (Long device : usedClients) {
                setState(conn, device, 2);
            }
            Logger.log("✅ 分配完成：共分配 " + targetSelldersUrls.size() + " 条 URL 给 " + clientCount + " 个 Client", WORKDIC);
        } finally {setState(conn, 0, 0);}
    }

    public static void assignSellersUrls(Connection conn) {
        if (!setState(conn, 0, 1)) {
            Logger.log("❌ 设置本机 serverDevice 占用状态失败，终止分配", WORKDIC);
            return;
        }

        try {
            List<Map<String, String>> sellerInfoList = pullIdentifierAndPageType(conn, "Sellers");
            List<Long> freeClients = pullClients(conn);

            if (sellerInfoList.isEmpty() || freeClients.isEmpty()) {
                Logger.log("⚠️ 没有可分配的 seller 信息 或 空闲 Client", WORKDIC);
                return;
            }

            List<String> sellerUrls = new ArrayList<>();
            for (Map<String, String> info : sellerInfoList) {
                String pageType = info.get("pageType");
                String identifier = info.get("identifier");
                String builtUrl = UrlBuildService.buildSellerSearchUrl(pageType, identifier);
                if (!builtUrl.isEmpty()) {
                    sellerUrls.add(builtUrl);
                }
            }

            int clientCount = freeClients.size();
            List<Long> usedClients = new ArrayList<>();

            for (int i = 0; i < sellerUrls.size(); i++) {
                long device = freeClients.get(i % clientCount);
                assignUrlToClient(conn, device, sellerUrls.get(i));
                if (!usedClients.contains(device)) {
                    usedClients.add(device);
                }
            }

            for (Long device : usedClients) {
                setState(conn, device, 3);
            }

            Logger.log("✅ 分配完成：共分配 " + sellerUrls.size() + " 条 URL 给 " + clientCount + " 个 Client", WORKDIC);
        } finally {
            setState(conn, 0, 0);
        }
    }

    public static void assignSellerCategoryUrls(Connection conn) {
        if (!setState(conn, 0, 1)) {
            Logger.log("❌ 设置本机 serverDevice 占用状态失败，终止分配", WORKDIC);
            return;
        }

        try {
            List<Map<String, String>> classificateInfoList = pullIdentifierAndPageTypeAndCP(conn, "Classificate");
            List<Long> freeClients = pullClients(conn);

            if (classificateInfoList.isEmpty() || freeClients.isEmpty()) {
                Logger.log("⚠️ 没有可分配的分类信息 或 空闲 Client", WORKDIC);
                return;
            }

            List<String> classifyUrls = new ArrayList<>();
            for (Map<String, String> info : classificateInfoList) {
                String pageType = info.get("pageType");
                String identifier = info.get("identifier");
                String cp = info.get("category_pv");

                String builtUrl = UrlBuildService.buildSellerClassificateUrl(pageType, identifier, cp);
                if (!builtUrl.isEmpty()) {
                    classifyUrls.add(builtUrl);
                }
            }

            int clientCount = freeClients.size();
            List<Long> usedClients = new ArrayList<>();

            for (int i = 0; i < classifyUrls.size(); i++) {
                long device = freeClients.get(i % clientCount);
                assignUrlToClient(conn, device, classifyUrls.get(i));
                if (!usedClients.contains(device)) {
                    usedClients.add(device);
                }
            }

            for (Long device : usedClients) {
                setState(conn, device, 4);
            }

            Logger.log("✅ 分类 URL 分配完成：共分配 " + classifyUrls.size() + " 条 URL 给 " + clientCount + " 个 Client", WORKDIC);
        } finally {
            setState(conn, 0, 0);
        }
    }

    public static void productTagNeat(Connection conn) {
        if (!setState(conn, 0, 1)) {
            Logger.log("❌ 设置本机 serverDevice 占用状态失败，终止清洗", WORKDIC);
            return;
        }

        try {
            int rows = DatabaseService.rebuildTaskProductTable(conn);
            if (rows >= 0) {
                Logger.log("✅ productTagNeat 成功执行，插入记录数：" + rows, WORKDIC);
            }
        } finally {
            setState(conn, 0, 0);
        }
    }

    public static void assignProductUrls(Connection conn) {
        if (!setState(conn, 0, 1)) {
            Logger.log("❌ 设置本机 serverDevice 占用状态失败，终止分配", WORKDIC);
            return;
        }

        try {
            // 从 TaskProduct 表获取 pageType 和 id
            List<Map<String, String>> productInfoList = pullPageTypeAndId(conn);
            List<Long> freeClients = pullClients(conn);

            if (productInfoList.isEmpty() || freeClients.isEmpty()) {
                Logger.log("⚠️ 没有可分配的商品信息 或 空闲 Client", WORKDIC);
                return;
            }

            List<String> productUrls = new ArrayList<>();
            for (Map<String, String> info : productInfoList) {
                String pageType = info.get("pageType");
                String id = info.get("id");

                String builtUrl = UrlBuildService.buildProductUrl(pageType, id);
                if (!builtUrl.isEmpty()) {
                    productUrls.add(builtUrl);
                }
            }

            int clientCount = freeClients.size();
            List<Long> usedClients = new ArrayList<>();

            for (int i = 0; i < productUrls.size(); i++) {
                long device = freeClients.get(i % clientCount);
                assignUrlToClient(conn, device, productUrls.get(i));
                if (!usedClients.contains(device)) {
                    usedClients.add(device);
                }
            }

            for (Long device : usedClients) {
                setState(conn, device, 6);
            }

            Logger.log("✅ 商品 URL 分配完成：共分配 " + productUrls.size() + " 条 URL 给 " + clientCount + " 个 Client", WORKDIC);
        } finally {
            setState(conn, 0, 0);
        }
    }

    public static void assignProductUrls_c(Connection conn) {
        if (!setState(conn, 0, 1)) {
            Logger.log("❌ 设置本机 serverDevice 占用状态失败，终止分配", WORKDIC);
            return;
        }

        try {
            // 从 TaskProduct 表获取 pageType 和 id
            List<Map<String, String>> productInfoList = pullPageTypeAndId(conn);
            List<Long> freeClients = pullClients(conn);

            if (productInfoList.isEmpty() || freeClients.isEmpty()) {
                Logger.log("⚠️ 没有可分配的商品信息 或 空闲 Client", WORKDIC);
                return;
            }

            List<String> productUrls = new ArrayList<>();
            for (Map<String, String> info : productInfoList) {
                String pageType = info.get("pageType");
                String id = info.get("id");

                String builtUrl = UrlBuildService.buildProductUrl(pageType, id);
                if (!builtUrl.isEmpty()) {
                    productUrls.add(builtUrl);
                }
            }

            int clientCount = freeClients.size();
            List<Long> usedClients = new ArrayList<>();

            for (int i = 0; i < productUrls.size(); i++) {
                long device = freeClients.get(i % clientCount);
                assignUrlToClient(conn, device, productUrls.get(i));
                if (!usedClients.contains(device)) {
                    usedClients.add(device);
                }
            }

            for (Long device : usedClients) {
                setState(conn, device, 7);
            }

            Logger.log("✅ 商品 URL 分配完成：共分配 " + productUrls.size() + " 条 URL 给 " + clientCount + " 个 Client", WORKDIC);
        } finally {
            setState(conn, 0, 0);
        }
    }
}
