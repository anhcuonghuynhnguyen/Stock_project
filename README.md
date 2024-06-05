<h1>Hello Guys, Welcome to My First DE Project</h1>
In this project I will create an extraction, storage and BI system for stock data. The project will focus on the US stock market, I will expand other markets later, I hope you will accompany me.

- [A. Data Source And Design](#a-data-source-and-design)
  - [I. API Detail](#i-api-detail)
    - [1. sec-api.io](#1-sec-apiio)
    - [2. Alpha Vantage API for market-status](#2-alpha-vantage-api-for-market-status)
    - [3. Alpha Vantage API for news\_sentiment](#3-alpha-vantage-api-for-news_sentiment)
    - [4. Polygon](#4-polygon)
  - [II. Extract from JSON](#ii-extract-from-json)
    - [1. Extract](#1-extract)
    - [2. Estimate](#2-estimate)
  - [III. ERD](#iii-erd)
  - [IV. Star Schema](#iv-star-schema)
  - [V. ETL Architecture](#v-etl-architecture)


# A. Data Source And Design
## I. API Detail
Đầu tiên chúng ta hãy cùng xem qua API được cung cấp là gì. 
### 1. sec-api.io
[sec-api.io](https://sec-api.io/docs/mapping-api/list-companies-by-exchange)
- API này giúp chúng ta truy xuất danh sách các công ty hiện đang có mặt trên các thị trường chứng khoán Nyse, Nasdaq, Nysemkt và Bats
- API này sẽ được extract mỗi tháng, mỗi khi extract là mất 4 lần (Nyse, Nasdaq, Nysemkt, Bats)
- Lưu ý: chỉ có 100 lần extract
- Dưới đây là các tham số API cần thiết để truy xuất danh sách các công ty có trên các thị trường chứng khoán NASDAQ, NYSE, NYSEMKT và NATS.
    - **exchange_name**: Tên của thị trường chứng khoán cần trích xuất công ty.
    - **api_key**: Khóa API để xác thực yêu cầu.
- Chỉ lấy các công ty của 4 sàn chứng khoán Hoa Kỳ là NYSE, NASDAQ, NYSEMKT (AMEX), BATS
- Dự kiến trong tương lai:
    - Thuộc tính isDelisted của các công ty có thể thay đổi → cập nhật giá trị của thuộc tính này cho công ty đó
    - Có công ty mới được thêm vào sàn giao dịch → thêm công ty mới vào
  
**Dưới đây là mô tả cho mỗi thuộc tính trong tập tin JSON được API trả về:**
  1. **name**: Tên của công ty.
  2. **ticker**: Ký hiệu duy nhất được sử dụng để nhận biết công ty trên sàn giao dịch chứng khoán, như "AAAB", "AAAP" hoặc "AABC".
  3. **cik**: Mã CIK (Central Index Key) được cấp cho công ty bởi Ủy ban Chứng khoán và Giao dịch (SEC). Ví dụ bao gồm "1066808", "1611787" hoặc "1024015".
  4. **cusip**: Số CUSIP (Committee on Uniform Securities Identification Procedures), một định danh duy nhất cho chứng khoán. Ví dụ bao gồm "007231103", "00790T100" hoặc "00431F105".
  5. **exchange**: Sàn giao dịch nơi cổ phiếu của công ty được giao dịch, như "NASDAQ".
  6. **isDelisted**: Một giá trị boolean cho biết liệu cổ phiếu của công ty đã bị loại khỏi sàn giao dịch hay không.
  7. **category**: Mô tả loại hoặc hạng mục của cổ phiếu của công ty, như "Cổ phiếu thường về nội địa" hoặc "Cổ phiếu thông thường ADR".
  8. **sector**: Ngành của công ty, ví dụ như "Dịch vụ Tài chính" hoặc "Chăm sóc sức khỏe".
  9. **industry**: Ngành cụ thể mà công ty hoạt động, như "Ngân hàng" hoặc "Công nghệ Sinh học".
  10. **sic**: Mã phân loại ngành công nghiệp tiêu chuẩn (SIC), phân loại loại hình kinh doanh chính của công ty. Ví dụ bao gồm "6022" hoặc "2834".
  11. **sicSector**: Mô tả ngành dựa trên mã SIC, như "Tài chính Bảo hiểm Và Bất động sản" hoặc "Sản xuất".
  12. **sicIndustry**: Xác định ngành dựa trên mã SIC, như "Ngân hàng Thương mại Nhà nước" hoặc "Các loại thuốc".
  13. **famaSector**: Có thể liên quan đến các ngành mô hình Fama-French.
  14. **famaIndustry**: Có thể liên quan đến các ngành mô hình Fama-French.
  15. **currency**: Đơn vị tiền tệ mà cổ phiếu của công ty được giao dịch, như "USD" cho đô la Mỹ hoặc "EUR" cho euro.
  16. **location**: Địa điểm của trụ sở công ty, như "Florida, Hoa Kỳ" hoặc "Pháp".
  17. **id**: Một định danh duy nhất cho mục nhập công ty, ví dụ như "3daf92ec6639eede5c282d6cd20f8342", "cf1185a716b218ee7db7d812695108eb", hoặc "800e1a7171119770ea03175a8f58830a".

### 2. Alpha Vantage API for market-status
[Alpha Vantage API](https://www.alphavantage.co/documentation/#market-status)
- API này giúp chúng ta truy xuất danh sách các thị trường chứng khoán trên toàn thế giới
- API này sẽ được extract mỗi tháng, nhưng có thể không cần thiết vì dữ liệu về thị trường và khu vực ít khi thay đổi
- Đây là các tham số API cần thiết để truy xuất trạng thái thị trường hiện tại của các sàn giao dịch chính cho cổ phiếu, ngoại hối và tiền điện tử trên toàn thế giới:
    - **function**: "MARKET_STATUS".
    - **apikey**: Khóa API để xác thực yêu cầu.
- Mọi thay đổi trong tương lai đều được Overwrite
  
**Dữ liệu này cung cấp thông tin về các khu vực và các sàn chứng khoán tại khu vực đó. Dưới đây là mô tả của các thuộc tính trong JSON:**
- **market_type**: Loại thị trường, ở đây là Equity (Chứng khoán).
- **region**: Khu vực của thị trường, ví dụ: Hoa Kỳ, Canada, Vương quốc Anh.
- **primary_exchanges**: Các sàn giao dịch chính của thị trường.
- **local_open**: Thời gian mở cửa địa phương của thị trường.
- **local_close**: Thời gian đóng cửa địa phương của thị trường.
- **current_status**: Tình trạng hiện tại của thị trường (bỏ qua)

### 3. Alpha Vantage API for news_sentiment
[Alpha Vantage API](https://www.alphavantage.co/documentation/#news-sentiment)
- API này giúp chúng ta truy xuất dữ liệu tin tức về thị trường, cùng với dữ liệu sentiment từ một loạt các nguồn tin hàng đầu trên toàn thế giới.
- API này sẽ được extract mỗi cuối ngày. Mỗi lần extract sẽ lấy tất cả các bài viết được xuất bản trong ngày tại thị trường Mỹ
- Dưới đây là các tham số API cần thiết để truy xuất dữ liệu tin tức về thị trường, cùng với dữ liệu sentiment từ một loạt các nguồn tin hàng đầu trên toàn thế giới, bao gồm cổ phiếu, tiền điện tử, ngoại hối và một loạt các chủ đề như chính sách tài khóa, sáp nhập và thâu tóm, IPO, v.v.
    - **function**: "NEWS_SENTIMENT".
    - **tickers**: Các ký hiệu cổ phiếu / tiền điện tử / ngoại hối. Ví dụ: tickers=IBM sẽ lọc các bài viết đề cập đến ký hiệu IBM; tickers=COIN,CRYPTO:BTC,FOREX:USD sẽ lọc các bài viết đồng thời đề cập đến Coinbase (COIN), Bitcoin (CRYPTO:BTC), và Đô la Mỹ (FOREX:USD) trong nội dung của chúng. **`<lấy các bài báo liên quan đến thị trường chứng khoán>`**
    - **topics**: Các chủ đề tin tức quan tâm. Ví dụ: topics=technology sẽ lọc các bài viết viết về lĩnh vực công nghệ; topics=technology,ipo sẽ lọc các bài viết đồng thời nói về công nghệ và IPO trong nội dung của chúng. Dưới đây là danh sách đầy đủ các chủ đề được hỗ trợ.
        - Blockchain: `blockchain`
        - Earnings: `earnings`
        - IPO: `ipo`
        - Mergers & Acquisitions: `mergers_and_acquisitions`
        - Financial Markets: `financial_markets`
        - Economy - Fiscal Policy (e.g., tax reform, government spending): `economy_fiscal`
        - Economy - Monetary Policy (e.g., interest rates, inflation): `economy_monetary`
        - Economy - Macro/Overall: `economy_macro`
        - Energy & Transportation: `energy_transportation`
        - Finance: `finance`
        - Life Sciences: `life_sciences`
        - Manufacturing: `manufacturing`
        - Real Estate & Construction: `real_estate`
        - Retail & Wholesale: `retail_wholesale`
        - Technology: `technology`
        - **`<sử dụng tất cả các topic>`**
    - **time_from** và **time_to**: Phạm vi thời gian của các bài báo, theo định dạng YYYYMMDDTHHMM. Ví dụ: time_from=20220410T0130. Nếu time_from được chỉ định nhưng time_to bị thiếu, API sẽ trả về các bài viết được xuất bản giữa giá trị time_from và thời gian hiện tại. `**<bắt đầu truy vấn dữ liệu từ tháng 1 năm 2024>**`
    - **sort**: Theo mặc định, sort=LATEST và API sẽ trả về các bài viết mới nhất đầu tiên. Cũng có thể thiết lập sort=EARLIEST hoặc sort=RELEVANCE. **`<sử dụng LATEST>`**
    - **limit**: Theo mặc định, limit=50 và API sẽ trả về tối đa 50 kết quả phù hợp. Cũng có thể thiết lập limit=1000 để xuất tối đa 1000 kết quả. **`<sử dụng 1000>`**
    - **apikey**: Khóa API để xác thực yêu cầu.

**File JSON trả về (Frequently Insert)**
Nguồn cung cấp tin tức hoặc phân tích thị trường, có thể là một trang web hoặc một dịch vụ.
- **title**: Tiêu đề của mục.
- **url**: Đường dẫn đến bài viết hoặc nguồn cung cấp.
- **time_published**: Thời gian xuất bản.
- **authors**: Tác giả của mục.
- **summary**: Tóm tắt nội dung của mục.
- **source**: Nguồn cung cấp thông tin.
- **source_domain**: Tên miền của nguồn cung cấp.
- **topics**: Danh sách các chủ đề liên quan đến mục.
    - **topic**: Chủ đề.
    - **relevance_score**: Điểm độ liên quan của bài viết đến chủ đề đó.
- **overall_sentiment_score**: Điểm cảm xúc tổng thể của bài báo.
- **overall_sentiment_label**: Nhãn mô tả cảm xúc tổng thể của bài báo.
- **ticker_sentiment**: Danh sách các cổ phiếu được đề cập trong bài báo và điểm cảm xúc của chúng.
    - **ticker**: Mã cổ phiếu.
    - **relevance_score**: Độ liên quan của bài báo đến cổ phiếu đó.
    - **ticker_sentiment_score**: Điểm cảm xúc của bài báo cho cổ phiếu đó.
    - **ticker_sentiment_label**: Nhãn mô tả cảm xúc của bài báo cho cổ phiếu đó.
- **Chỉ số cảm xúc (sentiment_score_definition):**
    | sentiment_score_definition | x <= -0.35 | -0.35 < x <= -0.15 | -0.15 < x < 0.15 | 0.15 <= x < 0.35 | x >= 0.35 |
    | -------------------------- | ---------- | ------------------ | ---------------- | ---------------- | --------- |
    | Label                      | Bearish    | Somewhat-Bearish   | Neutral          | Somewhat_Bullish | Bullish   |
- **Chỉ số liên quan (relevance_score_definition):** 0 < x <= 1, điểm số càng cao thì độ liên quan càng cao.

### 4. Polygon
[Polygon](https://polygon.io/docs/stocks/get_v2_aggs_ticker__stocksticker__range__multiplier___timespan___from___to)
- API này giúp chúng ta truy xuất giá mở cửa, cao nhất, thấp nhất và đóng cửa hàng ngày (OHLC) cho toàn bộ thị trường chứng khoán tại Mỹ.
- API này sẽ được extract mỗi đầu ngày, vì dữ liệu ngày hôm nay ngày mai mới có thể truy cập (hạn chế của việc sử dụng free)
    - Mỗi lần extract sẽ lấy chỉ số của tất cả các cổ phiếu hiện đang giao dịch tại thị trường Mỹ
    - Giới hạn gọi 5 API trong 1 phút
- Đây là các tham số API cần thiết để truy xuất dữ liệu:
    - **date**: Ngày bắt đầu cho khoảng thời gian tổng hợp.
    - **adjusted**: Tuỳ chọn, mặc định là "true". Đặt "adjusted=false" để truy vấn dữ liệu gốc (chưa điều chỉnh). Adjusted có nghĩa là giá của cổ phiếu đã được điều chỉnh dựa trên các hoạt động chia cổ tức . **`<sử dụng cả true và false> ⇒ 2 bảng giá của cổ phiếu`**
    - apikey: Khóa API để xác thực yêu cầu.

**File JSON trả về (Frequently Insert)**
**Thông tin này là về dữ liệu giao dịch của cổ phiếu sau 1 ngày. Dưới đây là mô tả của các thuộc tính trong JSON:**
- **T**: Mã cổ phiếu.
- **o**: Giá mở cửa.
- **h**: Giá cao nhất.
- **l**: Giá thấp nhất.
- **c**: Giá đóng cửa.
- **v**: Khối lượng giao dịch.

## II. Extract from JSON
### 1. Extract 
Dữ liệu lấy được từ Data Source ta tiến hành xử lý các file JSON và Insert vào các bảng có trong Data Staging.
Từ các file JSON trên ta trích xuất ra các bảng thông tin:
- Bảng khu vực:
    - region: khu vực
    - local_open: Thời gian mở cửa địa phương.
    - local_close: Thời gian đóng cửa địa phương.
- Bảng thị trường:
    - primary_exchanges: Thị trường.
    - region: Khu vực của thị trường.
- Bảng giao dịch hằng ngày:
    - symbol: Mã cổ phiếu.
    - open: Giá mở cửa.
    - high: Giá cao nhất.
    - low: Giá thấp nhất.
    - close: Giá đóng cửa.
    - volume: Khối lượng giao dịch.
- Bảng thông tin về công ty:
    - name: Tên của công ty.
    - ticker: Ký hiệu duy nhất được sử dụng để nhận biết công ty trên sàn giao dịch chứng khoán.
    - exchange: Sàn giao dịch nơi cổ phiếu của công ty được giao dịch.
    - isDelisted: Một giá trị boolean cho biết liệu cổ phiếu của công ty đã bị loại khỏi sàn giao dịch hay không.
    - category: Mô tả loại hoặc hạng mục của cổ phiếu của công ty.
    - sector: Ngành của công ty.
    - industry: Ngành cụ thể mà công ty hoạt động.
    - sic: Mã phân loại ngành công nghiệp tiêu chuẩn (SIC), phân loại loại hình kinh doanh chính của công ty. 
    - currency: Đơn vị tiền tệ mà cổ phiếu của công ty được giao dịch.
    - location: Địa điểm của trụ sở công ty.
    - id: Một định danh duy nhất cho mục nhập công ty
- Bảng các bài báo:
    - title: Tiêu đề của mục.
    - url: Đường dẫn đến bài viết hoặc nguồn cung cấp.
    - time_published: Thời gian xuất bản.
    - authors: Tác giả của mục.
    - summary: Tóm tắt nội dung của mục.
    - source: Nguồn cung cấp thông tin.
    - source_domain: Tên miền của nguồn cung cấp.
    - overall_sentiment_score: Điểm cảm xúc tổng thể của bài báo.
    - overall_sentiment_label: Nhãn mô tả cảm xúc tổng thể của bài báo.
- Bảng các topic:
    - topics: Chủ đề.
- Bảng trung gian của bài báo và topic:
    - topic: Chủ đề.
    - news: Bài báo.
    - relevance_score: Điểm độ liên quan của bài viết đến chủ đề đó.
- Bảng trung gian của bài báo và cổ phiếu của công ty:
    - ticker: Mã cổ phiếu.
    - news: Bài báo.
    - relevance_score: Điểm độ liên quan của bài báo đến cổ phiếu đó.
    - ticker_sentiment_score: Điểm cảm xúc của cổ phiếu đó.
    - ticker_sentiment_label: Nhãn mô tả cảm xúc của cổ phiếu đó.

### 2. Estimate 
Để ước tính lượng dữ liệu được đưa vào hệ thống theo 4Vs của big data, chúng ta sẽ xem xét các thông tin về số lượng dòng dữ liệu, số lượng bài báo và tần suất cập nhật để tính toán.

- **Volume (Khối lượng)**:
    
    Để ước tính dung lượng cho khối lượng dữ liệu được mô tả, ta cần xem xét các yếu tố sau:
    
    - a. **Dung lượng trung bình của mỗi dòng dữ liệu:**
        - Bảng giao dịch hằng ngày: Mỗi dòng dữ liệu bao gồm thông tin về một giao dịch, bao gồm mã chứng khoán, giá, khối lượng, thời gian giao dịch, v.v. Giả sử trung bình mỗi dòng dữ liệu có thể cần khoảng 100 byte ~ 0.1 KB để lưu trữ.
        - Bảng các bài báo: Mỗi bài báo bao gồm tiêu đề, nội dung, tác giả, ngày xuất bản, tóm tắt v.v. Giả sử trung bình mỗi bài báo có thể cần khoảng 1 kilobyte (KB) để lưu trữ.
        - Giả sử mỗi khi bảng các bài báo được ghi thì NEWS_Fact_Table cũng được cập nhật với môi dòng là 0,5 KB.
    - b. **Tần suất truy vấn dữ liệu:**
        - Bảng giao dịch hằng ngày: 10400 dòng dữ liệu được truy vấn mỗi ngày.
        - Bảng các bài báo: 10000 bài viết được truy vấn mỗi ngày.
        - Bảng khu vực: Cập nhật mỗi tháng.
        - Bảng thông tin về công ty: Cập nhật mỗi tháng.
    - c. **Tính toán:**
        - **Bảng giao dịch hằng ngày:**
            - Dung lượng trung bình mỗi dòng: 0.1 KB
            - Số lượng dòng dữ liệu mỗi ngày: 10400
            - Dung lượng lưu trữ cần thiết: 0.1 KB/dòng * 10400 dòng/ngày ≈ 1040 KB
        - **Bảng các bài báo:**
            - Dung lượng trung bình mỗi bài báo: 1 KB
            - Mỗi bài báo liên quan tới 3 cổ phiếu chứng khoán: 0.5 KB
            - Số lượng bài báo mỗi ngày: 1000
            - Dung lượng lưu trữ cần thiết: 1.5 KB/bài báo * 10000 bài báo/ngày ≈ 15000 KB
        - **Tổng dung lượng lưu trữ cần thiết trong một ngày là: 16040 KB ~ 15.66 MB**
        - **Tổng dung lượng lưu trữ cần thiết trong một tháng là: 469.92 MB**
        - **Tổng dung lượng lưu trữ cần thiết trong một năm là: 5,639.06 MB ~ 5.5 GB**
- **Velocity (Tốc độ)**:
    - **Giao dịch cổ phiếu:** Mỗi giây, hàng triệu giao dịch cổ phiếu được thực hiện trên các sàn giao dịch toàn cầu. Dữ liệu giao dịch này cần được thu thập và xử lý trong thời gian thực để cung cấp cho các nhà đầu tư thông tin cập nhật về giá cả và khối lượng giao dịch.
    - **Tin tức và dữ liệu tài chính:** Các nguồn tin tức và dữ liệu tài chính liên tục tạo ra một lượng lớn dữ liệu mới, bao gồm bài báo, báo cáo, phân tích và ý kiến. Dữ liệu này cần được xử lý và phân tích nhanh chóng để giúp các nhà đầu tư đưa ra quyết định sáng suốt.
- **Variety (Đa dạng)**:
    - **Dữ liệu giá cả:** Giá cổ phiếu theo thời gian thực và lịch sử, khối lượng giao dịch, giá mở, cao, thấp, đóng cửa, biến động giá
    - **Dữ liệu thị trường:** Chỉ số thị trường (VNIndex, VIX, v.v.), Tin tức và thông báo công ty, Báo cáo tài chính và phân tích, Thông tin về thị trường, Các bài báo với nhiều chủ đề.
- **Veracity (Tính chính xác)**:
    - Nguồn dữ liệu được lấy từ các tổ chức uy tín như:
        - **SEC EDGAR** là viết tắt của **Electronic Data Gathering, Analysis, and Retrieval**, là một hệ thống điện tử được Ủy ban Chứng khoán và Giao dịch Hoa Kỳ (SEC) sử dụng để thu thập, phân tích và truy xuất các tài liệu được nộp bởi các công ty đại chúng và các tổ chức khác.
        - Alpha Vantage Inc. là nhà cung cấp hàng đầu các API có thể truy cập cho dữ liệu thị trường tài chính bao gồm cổ phiếu, FX và tiền tệ kỹ thuật số/tiền điện tử.
        - Polygon.io là một công ty cung cấp dữ liệu và API cho thị trường tài chính được thành lập vào năm 2017 hợp tác với một số tổ chức uy tín trong ngành tài chính, bao gồm Google, Microsoft, Goldman Sachs và Morgan Stanley.
        - 
## III. ERD
- Cấu trúc CSDL của data staging được extract vào trước khi xử lý data để đưa vào Data Warehouse
<img src="images\ERD.png" alt="ERD">

## IV. Star Schema
- Business Requirement #1
    Tạo Data Mart từ CSDL để theo dõi giá bán cũng như khối lượng cổ phiểu được giao dịch sau mỗi ngày giao dịch. 
- Business Requirement #2
    Tạo Data Mart từ CSDL để theo dõi các chuyên gia nói gì về từng cổ phiếu sau mỗi ngày giao dịch. 

<img src="images\starschema.png" alt="starschema">

## V. ETL Architecture
<img src="images\ETL.png" alt="ETL">
