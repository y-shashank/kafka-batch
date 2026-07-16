# frozen_string_literal: true

require "cgi"
require "oj"

module KafkaBatch
  class Web
    # Serves the built React SPA from lib/kafka_batch/web/public.
    module Assets
      PUBLIC_DIR = File.expand_path("public", __dir__).freeze

      CONTENT_TYPES = {
        ".html" => "text/html; charset=utf-8",
        ".js"   => "application/javascript; charset=utf-8",
        ".css"  => "text/css; charset=utf-8",
        ".svg"  => "image/svg+xml",
        ".json" => "application/json",
        ".png"  => "image/png",
        ".ico"  => "image/x-icon",
        ".woff" => "font/woff",
        ".woff2" => "font/woff2",
        ".map"  => "application/json"
      }.freeze

      module_function

      def public_dir
        PUBLIC_DIR
      end

      def serve_file(path_info)
        rel = path_info.sub(%r{\A/}, "")
        return nil if rel.empty? || rel.include?("..")

        full = File.join(PUBLIC_DIR, rel)
        return nil unless full.start_with?(PUBLIC_DIR) && File.file?(full)

        ext  = File.extname(full)
        type = CONTENT_TYPES[ext] || "application/octet-stream"
        body = File.binread(full)
        cache = ext == ".html" ? "no-store" : "public, max-age=31536000, immutable"
        [200, { "content-type" => type, "cache-control" => cache, "content-length" => body.bytesize.to_s }, [body]]
      end

      # SPA shell: inject mount basename + CSRF so the client can call /api/*.
      def spa_shell(script_name:, csrf_token:, favicon_data_uri:)
        index = File.join(PUBLIC_DIR, "index.html")
        unless File.file?(index)
          body = <<~HTML
            <!DOCTYPE html>
            <html lang="en"><head><meta charset="utf-8"><title>KafkaBatch</title></head>
            <body>
              <h1>KafkaBatch UI not built</h1>
              <p>Run <code>cd frontend &amp;&amp; npm ci &amp;&amp; npm run build</code> in the gem source tree.</p>
            </body></html>
          HTML
          return [200, { "content-type" => "text/html; charset=utf-8", "cache-control" => "no-store" }, [body]]
        end

        mount = script_name.to_s.empty? ? "" : script_name.to_s
        base  = mount.empty? ? "/" : "#{mount}/"
        html  = File.read(index)
        boot  = <<~JS.rstrip
          <script>window.__KB_MOUNT__=#{Oj.dump(mount, mode: :compat)};window.__KB_CSRF__=#{Oj.dump(csrf_token.to_s, mode: :compat)};</script>
          <base href="#{CGI.escapeHTML(base)}">
          <link rel="icon" type="image/svg+xml" href="#{favicon_data_uri}">
        JS
        html = if html.include?("<head>")
          html.sub("<head>", "<head>\n#{boot}")
        else
          "#{boot}#{html}"
        end
        [200, { "content-type" => "text/html; charset=utf-8", "cache-control" => "no-store" }, [html]]
      end
    end
  end
end
