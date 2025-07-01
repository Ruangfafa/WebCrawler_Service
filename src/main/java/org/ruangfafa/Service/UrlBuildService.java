package org.ruangfafa.Service;

public class UrlBuildService {
    private static final String WORKDIC = "UrlBuildService.java";
    public static String buildSellerSearchUrl(String pageType, String identifier) {
        switch (pageType) {
            case "tb_c2c_1": case "tb_c2c_2": case "tb_c2c_ice":
                return "https://shop" + identifier + ".taobao.com/search.htm";
            case "tm":
                return "https://" + identifier + ".tmall.com/search.htm";
            case "tm_global":
                return "https://" + identifier + ".tmall.hk/search.htm";
            case "jd_zy_922474": case "jd_zy_401022": case "jd_fs_922474":
                return "https://mall.jd.com/view_search-" + identifier + "-0-5-1-24-1.html";
            default:
                Logger.log("❌ 未知的页面类型", WORKDIC);
                return "";
        }
    }

    public static String buildSellerClassificateUrl(String pageType, String identifier, String cp) {
        switch (pageType) {
            case "tb_c2c_1": case "tb_c2c_2":
                return "https://shop" + identifier + ".taobao.com/category-" + cp + ".htm";
            case "tm":
                if (cp.contains("_:_")) {
                    String[] parts = cp.split("_:_", 2);
                    String prefix = parts[0];
                    String value = parts[1];

                    if (prefix.equals("p")) {
                        return "https://" + identifier + ".tmall.com/search.htm?pv=" + value;
                    } else if (prefix.equals("c")) {
                        return "https://" + identifier + ".tmall.com/category-" + value + ".htm";
                    } else {
                        Logger.log("⚠️ 未知的 tm category_pv 前缀: " + prefix, WORKDIC);
                        return "";
                    }
                } else {
                    Logger.log("⚠️ 无效的 tm category_pv 格式: " + cp, WORKDIC);
                    return "";
                }
            case "tm_global":
                if (cp.contains(":")) {
                    return "https://" + identifier + ".tmall.hk/search.htm?pv=" + cp;
                } else {
                    return "https://" + identifier + ".tmall.hk/category-" + cp + ".htm";
                }
            case "jd_zy_922474": case "jd_zy_401022": case "jd_fs_922474":
                return "https://mall.jd.com/view_search-" + identifier + "-" + cp + "-99-1-20-1.html";
            default:
                Logger.log("❌ 未知的页面类型", WORKDIC);
                return "";
        }
    }
}
