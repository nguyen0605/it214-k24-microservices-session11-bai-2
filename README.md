# BÀI TẬP 2: XÂY DỰNG API ĐẶT HÀNG BẤT ĐỒNG BỘ (EVENT PRODUCER)

## 1. Giới thiệu kiến trúc
Ứng dụng sử dụng **Spring WebFlux** kết hợp với **Spring Kafka Reactive** nhằm cung cấp endpoint non-blocking hiệu năng cao, giảm tải cho luồng HTTP khi xử lý các giao dịch lớn.

## 2. Giải quyết vấn đề Partition Key (BUG-03)
- Đề bài yêu cầu topic `storex-order-events` có 5 partitions.
- Để đảm bảo tất cả các sự kiện liên quan đến cùng một đơn hàng (`orderId`) luôn được ghi vào cùng một Partition (đảm bảo tính thứ tự - ordering guarantee), chúng ta truyền `orderId` làm khóa (Key) vào `ProducerRecord`.
- Kafka sẽ dựa trên Hash của `Key` này để phân phối vào partition tương ứng, giúp các tiến trình tiêu thụ sự kiện (Consumer) xử lý đúng tuần tự trạng thái đơn hàng.