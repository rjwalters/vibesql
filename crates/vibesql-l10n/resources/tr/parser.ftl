# VibeSQL Ayrıştırıcı/Sözcük Çözümleyici Yerelleştirme - Türkçe
# Bu dosya, sözcük çözümleyici ve ayrıştırıcı hataları için tüm kullanıcı odaklı metinleri içerir.

# =============================================================================
# Sözcük Çözümleyici Hata Başlığı
# =============================================================================

lexer-error-at-position = Konum { $position }'de sözcük çözümleyici hatası: { $message }

# =============================================================================
# Metin Sabiti Hataları
# =============================================================================

lexer-unterminated-string = Sonlandırılmamış metin sabiti

# =============================================================================
# Tanımlayıcı Hataları
# =============================================================================

lexer-unterminated-delimited-identifier = Sonlandırılmamış sınırlandırılmış tanımlayıcı
lexer-empty-delimited-identifier = Boş sınırlandırılmış tanımlayıcıya izin verilmez

# =============================================================================
# Sayı Sabiti Hataları
# =============================================================================

lexer-invalid-scientific-notation = Geçersiz bilimsel gösterim: 'E' sonrasında rakam bekleniyor

# =============================================================================
# Yer Tutucu Hataları
# =============================================================================

lexer-expected-digit-after-dollar = Numaralı yer tutucu için '$' sonrasında rakam bekleniyor
lexer-invalid-numbered-placeholder = Geçersiz numaralı yer tutucu: ${ $placeholder }
lexer-numbered-placeholder-zero = Numaralı yer tutucu $1 veya daha yüksek olmalıdır ($0 yok)
lexer-expected-identifier-after-colon = Adlandırılmış yer tutucu için ':' sonrasında tanımlayıcı bekleniyor

# =============================================================================
# Değişken Hataları
# =============================================================================

lexer-expected-variable-after-at-at = @@ sonrasında değişken adı bekleniyor
lexer-expected-variable-after-at = @ sonrasında değişken adı bekleniyor

# =============================================================================
# Operatör Hataları
# =============================================================================

lexer-unexpected-pipe = Beklenmeyen karakter: '|' ('||' mi demek istediniz?)

# =============================================================================
# Genel Hatalar
# =============================================================================

lexer-unexpected-character = Beklenmeyen karakter: '{ $character }'
