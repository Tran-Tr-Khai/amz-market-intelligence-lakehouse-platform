# Amazon Market Research — Business Intelligence Questions

> **Vai trò:** Senior Data Analyst
> **Mục tiêu:** Xác định bộ câu hỏi phân tích trọng tâm giúp ra quyết định kinh doanh
> dựa trên dữ liệu thực từ Amazon.

---

## Tổng quan bài toán

Nghiên cứu thị trường Amazon không phải là thu thập dữ liệu — mà là **trả lời đúng câu hỏi đúng lúc**.
Tài liệu này phân loại các câu hỏi theo 6 nhóm vấn đề cốt lõi, mỗi câu hỏi được gắn với:

- **Vấn đề cần giải quyết** — quyết định kinh doanh nào được unblock
- **Nguồn dữ liệu** — bảng Gold và cột cụ thể
- **Tín hiệu chính** — metric / điều kiện lọc

---

## Nhóm 1 — Cơ hội thị trường (Market Entry)

> **Bài toán:** Tôi muốn bán một sản phẩm mới trên Amazon.
> Keyword nào đáng để đi sâu vào sản phẩm — không scrape ASIN trước,
> chỉ dùng data search để sàng lọc keyword ngon trước.

---

### Q1.1 — Keyword nào có nhu cầu cao nhưng cạnh tranh thấp?

**Vấn đề giải quyết:** Tìm "entry point" — nơi có traffic nhưng chưa bị
ads / brand đè chết. Shortlist keyword trước khi đầu tư scrape ASIN.

**Nguồn dữ liệu:** `mart_keyword_daily`

| Cột | Vai trò |
|---|---|
| `total_monthly_sales_min` | Proxy cho tổng cầu của keyword |
| `sponsored_pct` | % kết quả là quảng cáo trả tiền → mức độ cạnh tranh paid |
| `avg_ratings_count` | Barrier to entry (sản phẩm có nhiều review → khó cạnh tranh) |
| `best_seller_count` | Số lượng Best Seller badge → ai đang thống trị |

**Công thức:** `opportunity_score = 0.5 × demand − 0.3 × competition − 0.2 × barrier`

**Tín hiệu mục tiêu:** `total_monthly_sales_min HIGH` + `sponsored_pct < 30%`
+ `avg_ratings_count < 500`

**Decision:** → **Shortlist keyword (Top 20–50)** để đưa vào các bước lọc tiếp theo.

---

### Q1.2 — Giá thị trường có "đủ biên lợi nhuận" không?

**Vấn đề giải quyết:** Tránh keyword "có demand nhưng không có tiền" —
loại sớm những keyword mà giá thị trường quá thấp hoặc bị cạnh tranh giá khốc liệt.

**Nguồn dữ liệu:** `mart_keyword_daily` + `fact_price_snapshot`

| Cột | Vai trò |
|---|---|
| `median_price` | Giá đại diện của keyword |
| `avg_price` | Tham khảo |
| `min_price`, `max_price` | Dải giá toàn bộ thị trường |
| `discount_pct_latest` | Áp lực giảm giá thực tế |
| `coupon_count` / `has_coupon` | Mức độ cạnh tranh giá qua coupon |

**Ước tính margin sơ bộ (keyword-level):**

```
estimated_margin = median_price − estimated_fee − estimated_ads − estimated_cost
```

> Giả định nhanh: Fee ≈ 30% price · Ads ≈ 10–15% price

**Tín hiệu mục tiêu:** `median_price > $20` (tuỳ ngành) + discount không quá cao
+ coupon không quá dày đặc

**Decision:** Loại keyword có giá quá thấp hoặc cạnh tranh giá quá khốc liệt.

---

### Q1.3 — Demand có ổn định hay chỉ là trend ảo?

**Vấn đề giải quyết:** Tránh "đâm đầu vào trend chết nhanh" —
ưu tiên keyword có demand thật, ổn định theo thời gian.

**Nguồn dữ liệu:** `mart_keyword_daily` (time series)

| Cột | Vai trò |
|---|---|
| `total_monthly_sales_min` | Demand theo thời gian |
| `date` | Time index |

**Công thức:**

```
stability_score = AVG(total_monthly_sales_min) / STDDEV(total_monthly_sales_min)
```

**Tín hiệu mục tiêu:** Demand không bị spike bất thường · variance thấp
· trend tăng nhẹ hoặc stable

**Decision:** Giữ keyword có demand "thật" — loại keyword spike rồi drop.

---

### Q1.4 — Keyword có bị ads "chiếm sóng" không?

**Vấn đề giải quyết:** Biết có thể win organic hay phải đốt tiền ads —
keywords bị ad saturation cao cần budget lớn hoặc nên tránh.

**Nguồn dữ liệu:** `mart_keyword_daily`

| Cột | Vai trò |
|---|---|
| `sponsored_pct` | % kết quả là quảng cáo trả tiền |
| `best_seller_count` | Mức độ brand dominance |

**Tín hiệu mục tiêu:**
- `sponsored_pct < 30%` → healthy — cơ hội SEO / organic
- `sponsored_pct 30–50%` → competitive
- `sponsored_pct > 50%` → red flag — pay-to-play

**Decision:** Ưu tiên keyword ads thấp + demand ổn.

---

### Q1.5 — Thị trường có bị "đè bởi brand lớn" không?

**Vấn đề giải quyết:** Tránh keyword bị monopoly bởi brand mạnh —
market phân mảnh mới có cơ hội cho người mới.

**Nguồn dữ liệu:** `mart_keyword_daily`

| Cột | Vai trò |
|---|---|
| `best_seller_count` | Số ASIN đang giữ Best Seller badge |
| `avg_ratings_count` | Độ mạnh trung bình của sản phẩm trong keyword |

**Tín hiệu mục tiêu:** `best_seller_count` thấp + `avg_ratings_count` thấp
→ market phân mảnh → dễ cạnh tranh

**Decision:** Ưu tiên market fragmented; tránh keyword bị monopoly.
