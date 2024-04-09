source = ["./build/eds-server-darwin"]
bundle_id = "io.shopmonkey.eds-server"

apple_id {
  username = "jhaynie@shopmonkey.io"
  password = "@env:AC_PASSWORD"
}

sign {
  application_identity = "Developer ID Application: Shopmonkey Inc. (VJTZ4HKFCU)"
}

zip {
  output_path = "eds-server-darwin.zip"
}