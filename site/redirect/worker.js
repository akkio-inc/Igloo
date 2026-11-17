// Sends plain-HTTP requests for the website to HTTPS. It runs only on the
// http:// routes in ../wrangler.https-redirect.jsonc, so HTTPS requests never
// reach it: they go straight to the site's static assets.
export default {
	fetch(request) {
		const url = new URL(request.url);
		url.protocol = 'https:';
		return Response.redirect(url.href, 301);
	},
};
