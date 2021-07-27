const textEncoder = new TextEncoder();

// Defined at https://www.backblaze.com/b2/docs/string_encoding.html.
const SAFE_BYTES = new Set([
  ".", "_", "-", "/", "~", "!", "$", "'", "(", ")", "*", ";", "=", ":", "@",
].map(c => c.charCodeAt(0)));

const isDigit = b => b >= 0x30 && b <= 0x39;
const isLcAlpha = b => b >= 0x61 && b <= 0x7a;
const isUcAlpha = b => b >= 0x41 && b <= 0x5a;

const encodeB2PathComponent = raw => {
  const bytes = textEncoder.encode(raw);
  return [...bytes]
    .map(b => SAFE_BYTES.has(b) || isDigit(b) || isLcAlpha(b) || isUcAlpha(b) ? String.fromCharCode(b) : `%${b.toString(16)}`)
    .join('');
};

addEventListener("fetch", (event) => {
  event.respondWith(
    handleRequest(event.request).catch(
      (err) => new Response(err.stack, { status: 500 })
    )
  );
});

const fetchOkJson = (url, opts) => fetch(url, opts).then(res => {
  if (res.ok) {
    return res.json();
  }
  return res.json().then(err => {
    throw new Error(`Network request failed with status ${res.status}: ${JSON.stringify(err, null, 2)}`);
  });
});

async function handleRequest(request) {
  const auth = request.headers.get("authorization");
  const contentType = request.headers.get("content-type") || "b2/x-auto";
  const url = new URL(request.url);
  const [bucket, ...keyParts] = url.pathname
    .split("/")
    .filter(c => c)
    .map(c => decodeURIComponent(c.replaceAll("+", " ")));
  const key = keyParts.join("/");
  const sha1 = url.searchParams.get("sha1");

  if (!sha1) {
    return new Response("SHA-1 required", {
      status: 400,
    });
  }

  const body = await request.arrayBuffer();
  console.debug("Received request:", {
    contentType,
    bucket,
    key,
    sha1,
    contentLength: body.byteLength,
  });

  const {
    accountId, 
    apiUrl, 
    authorizationToken,
  } = await fetchOkJson("https://api.backblazeb2.com/b2api/v2/b2_authorize_account", {
    headers: {
      Authorization: auth,
    },
  });
  console.debug("Authorised with B2:", {
    accountId,
    apiUrl,
  });

  const {
    buckets: [
      {
        bucketId,
      },
    ],
  } = await fetchOkJson(`${apiUrl}/b2api/v2/b2_list_buckets`, {
    method: "POST",
    headers: {
      Authorization: authorizationToken,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      accountId,
      bucketName: bucket,
    }),
  });
  console.debug("Found bucket:", {
    bucketId,
  });

  const {
    authorizationToken: uploadAuthorizationToken,
    uploadUrl,
  } = await fetchOkJson(`${apiUrl}/b2api/v2/b2_get_upload_url`, {
    method: "POST",
    headers: {
      Authorization: authorizationToken,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      bucketId,
    }),
  });
  console.debug("Registered upload:", {
    uploadUrl,
  });

  const uploadResult = await fetchOkJson(uploadUrl, {
    method: "POST",
    body,
    headers: {
      Authorization: uploadAuthorizationToken,
      "Content-Type": contentType,
      "X-Bz-Content-Sha1": sha1,
      "X-Bz-File-Name": encodeB2PathComponent(key),
    }
  });

  return new Response(JSON.stringify(uploadResult, null, 2), {
    headers: { "Content-Type": "application/json" },
  });
}
