# Distributed Task Queue

Bu proje, kullanıcıların görev ekleyebileceği, görevlerin çoklu çalışan düğümler arasında dağıtılarak eşzamanlı işlenebileceği, otomatik yeniden deneme, zaman aşımı ve planlama özelliklerine sahip dağıtık bir görev kuyruğu sistemi geliştirmeyi amaçlamaktadır. 

## Proje Yapısı

Proje aşağıdaki dosya yapısına sahiptir:

```
distributed-task-queue
├── cmd
│   └── taskqueueservice
│       └── main.go          # Uygulamanın giriş noktası
├── internal
│   ├── queue
│   │   ├── queue.go         # Görev kuyruğu yapısı
│   │   └── task.go          # Görev veri yapısı
│   ├── worker
│   │   ├── pool.go          # Çalışan havuzu yönetimi
│   │   └── worker.go        # Görev işleme mantığı
│   ├── scheduler
│   │   └── scheduler.go     # Zamanlayıcı
│   ├── retry
│   │   └── backoff.go       # Yeniden deneme mekanizması
│   └── metrics
│       └── collector.go      # Metrik toplama
├── api
│   ├── handler.go           # API isteklerini işleyen fonksiyonlar
│   └── router.go            # API rotaları
├── config
│   └── config.go            # Yapılandırma ayarları
├── pkg
│   ├── logger
│   │   └── logger.go        # Loglama yapısı
│   └── utils
│       └── helpers.go       # Yardımcı fonksiyonlar
├── go.mod                    # Go modül yönetimi
├── go.sum                    # Bağımlılık sürüm bilgileri
├── Makefile                  # Derleme ve çalıştırma komutları
└── README.md                 # Proje hakkında bilgi
```

## Kurulum

1. **Gereksinimler**: Go programlama dili yüklü olmalıdır. [Go İndirin](https:
2. **Bağımlılıkları Yükleyin**: Proje dizininde terminal açarak aşağıdaki komutu çalıştırın:
   ```
   go mod tidy
   ```
3. **Uygulamayı Başlatın**: Aşağıdaki komut ile uygulamayı başlatabilirsiniz:
   ```
   go run cmd/taskqueueservice/main.go
   ```

## Kullanım

- Görev eklemek için API endpoint'lerini kullanabilirsiniz. Detaylar için `api/handler.go` dosyasını inceleyin.
- Görev durumlarını sorgulamak için ilgili API endpoint'lerini kullanın.
