# 🚀 MySQL to Typesense CDC Sync

[![TypeScript](https://img.shields.io/badge/TypeScript-007ACC?style=flat-square&logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![Node.js](https://img.shields.io/badge/Node.js-43853D?style=flat-square&logo=node.js&logoColor=white)](https://nodejs.org/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat-square)](https://opensource.org/licenses/MIT)

Một microservice siêu nhẹ (lightweight) viết bằng TypeScript giúp đồng bộ dữ liệu từ **MySQL** sang **Typesense** theo thời gian thực (Realtime CDC). Dự án sử dụng MySQL Binlog để bắt các sự kiện thay đổi dữ liệu (Insert, Update, Delete) mà không làm ảnh hưởng đến hiệu năng của database chính.

## ✨ Tính năng nổi bật

Dịch vụ được thiết kế với kiến trúc **2 chế độ (Dual-Mode)** hoạt động độc lập, giúp an toàn trong vận hành và dễ dàng tích hợp vào luồng CI/CD:

1. **Initial Sync (Đồng bộ lần đầu):** Quét toàn bộ dữ liệu hiện có trong MySQL và đẩy lên Typesense theo lô (Bulk Import) cực nhanh, sử dụng Cursor-based pagination để tối ưu RAM.
2. **Realtime Sync (Đồng bộ thời gian thực):** Lắng nghe MySQL Binlog. Bất kỳ thay đổi nào (Thêm/Sửa/Xóa) dưới database đều được tự động phản ánh lên Typesense chỉ trong vài mili-giây.

### 📊 So sánh giải pháp: Khi nào nên sử dụng dự án này?

| Tiêu chí | `mysql2typesense` (Project này) | Hệ thống Debezium + Kafka + Connect |
| :--- | :--- | :--- |
| **Độ phức tạp** | Rất thấp. Cài đặt và chạy trong 5 phút. | Rất cao. Cần setup Zookeeper/KRaft, Kafka Cluster. |
| **Tiêu hao tài nguyên** | Nhẹ (~50-100MB RAM cho mode realtime). | Nặng (Yêu cầu JVM, tốn vài GB RAM tối thiểu). |
| **Mục đích sử dụng** | Các dự án vừa/nhỏ, Startup, hoặc MVP cần giải pháp Search Realtime nhanh gọn. | Hệ thống Enterprise siêu lớn, kiến trúc Event-Driven phức tạp với nhiều Consumer. |
| **Bảo trì & Tùy biến** | Dễ dàng sửa logic mapping bằng code TypeScript thuần. | Đòi hỏi kiến thức chuyên sâu về Kafka & Java. |

---

## 🛠 Yêu cầu hệ thống (Prerequisites)

1. **Node.js**: Phiên bản 16.x trở lên.
2. **Typesense**: Đang chạy và có API Key.
3. **MySQL**: Hỗ trợ MySQL 5.7+ hoặc 8.0+. **Bắt buộc phải bật Binlog với định dạng ROW.**
4. **Quyền truy cập**: Tài khoản MySQL cần có quyền `REPLICATION SLAVE` để đọc Binlog.


### Cấu hình MySQL Binlog
Thêm hoặc kiểm tra đoạn cấu hình sau trong file `my.cnf` (hoặc `my.ini` trên Windows) của MySQL:

```ini
[mysqld]
server-id        = 1
log_bin          = /var/log/mysql/mysql-bin.log
binlog_format    = ROW
binlog_row_image = FULL
expire_logs_days = 10
````

*(Lưu ý: Khởi động lại MySQL service sau khi thay đổi file cấu hình).*

-----

## 📦 Cài đặt

**1. Clone repository:**

```bash
git clone [https://github.com/duypv/mysql2typesense.git](https://github.com/duypv/mysql2typesense.git)
cd mysql2typesense
```

**2. Cài đặt dependencies:**

```bash
npm install
```

**3. Cấu hình biến môi trường:**
Copy file `.env.example` thành `.env` và điền thông tin hệ thống của bạn:

```bash
cp .env.example .env
```

*Ví dụ nội dung file `.env`:*

```env
# MySQL Config
DB_HOST=127.0.0.1
DB_USER=root
DB_PASS=your_secret_password
DB_NAME=my_database
DB_TABLE=users

# Typesense Config
TS_NODE_HOST=127.0.0.1
TS_NODE_PORT=8108
TS_NODE_PROTOCOL=http
TS_API_KEY=your_typesense_api_key
TS_COLLECTION=users
```

-----

## 🚀 Hướng dẫn sử dụng

Dự án được tách biệt thành 2 script riêng để bạn dễ dàng quản lý thông qua PM2, Docker, hoặc Kubernetes.

### Bước 1: Đồng bộ dữ liệu lần đầu (Initial Sync)

Chạy lệnh này **một lần duy nhất** khi bạn mới setup hệ thống hoặc khi cần reset lại toàn bộ dữ liệu search index. Dịch vụ sẽ đọc toàn bộ table và đẩy sang Typesense, sau đó tự động thoát.

```bash
npm run sync:initial
```

### Bước 2: Bật Đồng bộ thời gian thực (Realtime CDC)

Sau khi Initial Sync hoàn tất, khởi động tiến trình chạy ngầm này để lắng nghe các thay đổi tiếp theo từ MySQL.

```bash
npm run sync:realtime
```

*💡 Mẹo cho Production: Nên sử dụng [PM2](https://pm2.keymetrics.io/) để giữ cho tiến trình realtime luôn chạy:*

```bash
pm2 start npm --name "typesense-realtime" -- run sync:realtime
```

## Docker
Bạn cũng có thể chạy dịch vụ này thông qua Docker. Dưới đây là hướng dẫn cơ bản:
**1. Build Docker Image:**

```bash
docker build -t mysql2typesense .
```
**2. Chạy Container:**

```bashbash
docker run -d --name mysql2typesense \
  -e DB_HOST=your_mysql_host \
  -e DB_USER=your_mysql_user \
  -e DB_PASS=your_mysql_password \
  -e DB_NAME=your_mysql_database \
  -e DB_TABLE=your_mysql_table \
  -e TS_NODE_HOST=your_typesense_host \
  -e TS_NODE_PORT=your_typesense_port \
  -e TS_NODE_PROTOCOL=http \
  -e TS_API_KEY=your_typesense_api_key \
  -e TS_COLLECTION=your_typesense_collection \
  mysql2typesense
```

-----

## 🏗 Roadmap (Dự kiến phát triển)

  - [ ] Hỗ trợ mapping nhiều table (Multi-table sync) cùng lúc.
  - [ ] Lưu trạng thái Binlog Position vào Redis để phục hồi không mất dữ liệu khi crash (High Availability).
  - [ ] Thêm giao diện Dashboard đơn giản để theo dõi trạng thái đồng bộ, thống kê lỗi, và quản lý collection trên Typesense.
  - [ ] Hỗ trợ các database khác ngoài MySQL (PostgreSQL, MongoDB) thông qua plugin architecture.
  - [ ] Hỗ trợ Data Transformation (cho phép viết custom function để biến đổi dữ liệu trước khi đẩy sang Typesense).
  - [ ] Đóng gói sẵn thành Docker Image.
  - [ ] Tối ưu hiệu năng cho các trường hợp dữ liệu lớn (Hàng triệu bản ghi) và tần suất thay đổi cao.
  - [ ] Thêm tính năng Retry/Backoff khi gặp lỗi kết nối hoặc lỗi API từ Typesense.

## 🤝 Đóng góp (Contributing)

Mọi đóng góp (Pull Request, Report Bug, Feature Request) đều được chào đón\! Rất mong nhận được ý tưởng từ cộng đồng để làm cho công cụ này mạnh mẽ hơn.

Vui lòng tạo Issue trước khi gửi một Pull Request lớn để chúng ta cùng thảo luận nhé.

## 📄 License

Dự án được phân phối dưới giấy phép [MIT](https://www.google.com/search?q=LICENSE). Trân trọng cảm ơn sự quan tâm của bạn\!