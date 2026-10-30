// Site header: below 900 px the nav links collapse behind the menu button.
// The button toggles aria-expanded; Escape, a click outside, following a link or widening the window closes it.
const header = document.querySelector('.vx-header');
const button = header?.querySelector('.vx-header__menu');
const nav = header?.querySelector('.vx-header__nav');

if (header && button && nav) {
	header.classList.add('vx-js');
	const isOpen = () => button.getAttribute('aria-expanded') === 'true';
	const setOpen = (open) => {
		button.setAttribute('aria-expanded', String(open));
		button.setAttribute('aria-label', open ? 'Close menu' : 'Menu');
		header.classList.toggle('is-open', open);
	};

	button.addEventListener('click', () => setOpen(!isOpen()));
	nav.addEventListener('click', (event) => {
		if (event.target instanceof Element && event.target.closest('a')) setOpen(false);
	});
	document.addEventListener('keydown', (event) => {
		if (event.key !== 'Escape' || !isOpen()) return;
		setOpen(false);
		button.focus();
	});
	document.addEventListener('click', (event) => {
		if (isOpen() && event.target instanceof Node && !header.contains(event.target)) setOpen(false);
	});
	const wide = matchMedia('(min-width: 900px)');
	wide.addEventListener('change', (event) => {
		if (event.matches) setOpen(false);
	});
}

// Hero scene: the illustration, alive. A WebGL shader lays the art's own aurora (aurora.jpg) over its plate
// (plate.jpg: the art with the aurora taken out), ripples and flickers it, and reflects it in the river's ice,
// which a normal and a roughness map shape (ice-*.png). masks.png holds the sky, the river and the igloo;
// warm.png the art's warm reflection of the igloo. The poster, a still of the scene at the moment the
// animation starts, shows until the first frame is drawn, and for good without WebGL or with reduced motion.
// The scene runs only while it is on screen and the page is visible.
const SCENE = {
	aspect: 1.5, // the art is 3:2
	riverTop: 0.7973, // the far edge of the river, as a fraction of the art's height
	skyLow: 0.44, // the lowest sky that edge reflects
	stretch: 1.6, // how much faster the reflection climbs the sky toward the viewer (1 for a flat mirror)
	iglooMirror: 0.7287, // the water level beneath the igloo, which its reflection mirrors about
	horizon: 0.6, // the horizon of the ice's ground plane
	start: 30, // the moment the poster shows, in seconds
};

const SCENE_VERT = `attribute vec2 p; varying vec2 vUv; void main() { vUv = p * .5 + .5; gl_Position = vec4(p, 0., 1.); }`;
const SCENE_FRAG = `
precision highp float;
uniform sampler2D uPlate, uMasks, uWarm, uAurora, uIceN, uIceR;
uniform vec2 uRes, uFocus;
uniform float uTime, uAspect, uRiverTop, uSkyLow, uStretch, uIglooMirror, uHorizon;
varying vec2 vUv;

float hash(vec2 p) { return fract(sin(dot(p, vec2(127.1, 311.7))) * 43758.5453); }
float noise(vec2 p) {
  vec2 i = floor(p), f = fract(p); f = f * f * (3. - 2. * f);
  return mix(mix(hash(i), hash(i + vec2(1., 0.)), f.x), mix(hash(i + vec2(0., 1.)), hash(i + vec2(1., 1.)), f.x), f.y);
}
float fbm(vec2 p) { float v = 0., a = .5; for (int i = 0; i < 5; i++) { v += a * noise(p); p *= 2.03; a *= .5; } return v; }

// The aurora at image coordinates p (x 0..1 left to right, y 0..1 top to bottom) is the art's own, alive:
// its curtains ripple (auroraWarp: where the art is sampled), and rays run sideways along them and flicker
// while surges of brightness travel down their length (auroraLight).
vec2 auroraWarp(vec2 p, float t) {
  vec2 w = vec2(fbm(vec2(p.y * 3. + t * .11, p.x * 2. + 1.7)), fbm(vec2(p.x * 2.5 - t * .09, p.y * 3. + 4.2))) - .5;
  return p + w * vec2(.016, .01) + vec2(.004 * sin(p.y * 9. + t * .7), 0.);
}
float auroraLight(vec2 q, float t) {
  float rays = .55 + .9 * noise(vec2(q.x * 140. + t * 1.1 + 6. * fbm(vec2(q.x * 3., t * .2)), q.y * 2.));
  float surge = .7 + .6 * smoothstep(-.4, 1., sin(q.x * 7. - q.y * 4. - t * .8));
  return rays * surge;
}
vec3 aurora(vec2 p, float t) { vec2 q = auroraWarp(p, t); return texture2D(uAurora, q).rgb * auroraLight(q, t); }
vec3 screenBlend(vec3 a, vec3 b) { return 1. - (1. - a) * (1. - clamp(b, 0., 1.)); }

void main() {
  // Cover the canvas with the art, centred on uFocus where the screen crops it.
  float sa = uRes.x / uRes.y;
  vec2 iuv = vec2(vUv.x, 1. - vUv.y);
  if (sa > uAspect) iuv.y = uFocus.y + (iuv.y - .5) * (uAspect / sa);
  else iuv.x = uFocus.x + (iuv.x - .5) * (sa / uAspect);
  float t = uTime;
  vec3 base = texture2D(uPlate, iuv).rgb;
  vec3 masks = texture2D(uMasks, iuv).rgb;
  float sky = masks.r, water = masks.g;
  // The ice's normal and roughness maps, laid on the ground plane in perspective (x across the river, y away
  // from the viewer, 6 ground units to a tile). Sampled here, where every pixel runs the same code, so their
  // mipmaps work. A negative mip bias keeps hairline cracks sharp; toward the bottom it grows, as the art's
  // foreground ice is out of focus.
  float depth = 1. / max(iuv.y - uHorizon, .04);
  vec2 P = vec2((iuv.x - .5) * uAspect * depth, depth) / 6.;
  float focus = mix(-1., 3., smoothstep(.86, 1., iuv.y));
  vec3 iceN = texture2D(uIceN, P, focus).rgb * 2. - 1.;
  float iceR = texture2D(uIceR, P, focus).r;

  vec3 col = screenBlend(base, aurora(iuv, t) * sky);

  // The river is ice, and reflects the aurora: a mirror image of the sky (the far shore, uRiverTop, sees the
  // lowest visible sky, uSkyLow, and toward the viewer the ice sees higher sky, uStretch times as fast). Ice is
  // glossy, so each point sees a tall, narrow patch of sky, mostly above its mirror point: the curtains are
  // drawn out into soft columns, and the ice's ripples tilt the reflection and break it into bands. Trees and
  // mountains reflect over the sky, and so does the igloo, mirrored about the water level beneath it
  // (uIglooMirror), and the art's warm reflection of it, which the art draws wider than the igloo. The ice's
  // roughness dims the reflection and scatters some of the light, most of all from cracks and scratches.
  if (water > .01) {
    // (The steep walls of cracks and scratches would throw the reflection far off; their light comes from
    // their roughness instead.)
    vec2 d = clamp(iceN.xy, -.06, .06) * vec2(.45, -1.1);
    float rough = smoothstep(.1, .6, iceR), lines = smoothstep(.62, .9, iceR);
    vec2 r = vec2(iuv.x, uSkyLow - uStretch * (iuv.y - uRiverTop)) + d;
    vec2 m = vec2(iuv.x, 2. * uIglooMirror - iuv.y) + vec2(d.x, d.y / uStretch);
    float tall = .1 + .05 * rough + .04 * smoothstep(.86, 1., iuv.y);
    vec3 ra = vec3(0.);
    for (int j = 0; j < 8; j++) {
      float f = float(j);
      vec2 o = vec2((fract(f * .618) - .5) * .008, mix(.03, -tall, f / 7.));
      float igloo = texture2D(uMasks, clamp(m + vec2(o.x, o.y / uStretch), 0., 1.)).b;
      ra += aurora(r + o, t) * texture2D(uMasks, clamp(r + o, 0., 1.)).r * (1. - igloo);
    }
    ra *= (1. - texture2D(uWarm, iuv).r) * (.45 + 1.1 * smoothstep(-.012, .012, iceN.y)) * 1.5 / 8.;

    float warm = smoothstep(.05, .25, base.r - base.b);
    vec3 light = ra + base * warm * .5;
    vec3 ice = ra * mix(1., .55, rough)
             + rough * (1. - lines) * (light * .08 + vec3(.004, .006, .01))
             + lines * (vec3(.6, .72, .9) * .55 + light * .6);
    // The plate's ice is a little lighter than clear black ice; its warm reflections keep their brightness.
    col = screenBlend(col * mix(1., .88, water * (1. - warm)), ice * water);
  }
  gl_FragColor = vec4(col, 1.);
}`;

const sceneArt = document.querySelector('.vx-hero__art');
const sceneCanvas = sceneArt?.querySelector('.vx-hero__scene');
if (sceneArt && sceneCanvas && !matchMedia('(prefers-reduced-motion: reduce)').matches) {
	startScene(sceneArt, sceneCanvas).catch(() => sceneArt.classList.remove('is-live'));
}

async function startScene(art, canvas) {
	const gl = canvas.getContext('webgl', { alpha: false, antialias: false, depth: false });
	if (!gl) return;
	const compile = (type, src) => {
		const s = gl.createShader(type);
		gl.shaderSource(s, src);
		gl.compileShader(s);
		if (!gl.getShaderParameter(s, gl.COMPILE_STATUS)) throw new Error(gl.getShaderInfoLog(s) ?? 'shader');
		return s;
	};
	const prog = gl.createProgram();
	gl.attachShader(prog, compile(gl.VERTEX_SHADER, SCENE_VERT));
	gl.attachShader(prog, compile(gl.FRAGMENT_SHADER, SCENE_FRAG));
	gl.linkProgram(prog);
	if (!gl.getProgramParameter(prog, gl.LINK_STATUS)) throw new Error(gl.getProgramInfoLog(prog) ?? 'link');
	gl.useProgram(prog);
	gl.bindBuffer(gl.ARRAY_BUFFER, gl.createBuffer());
	gl.bufferData(gl.ARRAY_BUFFER, new Float32Array([-1, -1, 3, -1, -1, 3]), gl.STATIC_DRAW);
	const at = gl.getAttribLocation(prog, 'p');
	gl.enableVertexAttribArray(at);
	gl.vertexAttribPointer(at, 2, gl.FLOAT, false, 0, 0);
	const U = (name) => gl.getUniformLocation(prog, name);

	const load = (url) =>
		new Promise((resolve, reject) => {
			const im = new Image();
			im.decoding = 'async';
			im.onload = () => resolve(im);
			im.onerror = () => reject(new Error(url));
			im.src = url;
		});
	const d = canvas.dataset;
	const images = await Promise.all([d.plate, d.masks, d.warm, d.aurora, d.iceNormal, d.iceRoughness].map(load));
	// The ice maps lie on the ground, seen at a grazing angle: they tile, and are mipmapped and anisotropic.
	const aniso = gl.getExtension('EXT_texture_filter_anisotropic');
	images.forEach((im, unit) => {
		const tile = unit >= 4;
		gl.activeTexture(gl.TEXTURE0 + unit);
		gl.bindTexture(gl.TEXTURE_2D, gl.createTexture());
		gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGB, gl.RGB, gl.UNSIGNED_BYTE, im);
		const wrap = tile ? gl.REPEAT : gl.CLAMP_TO_EDGE;
		gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, wrap);
		gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, wrap);
		gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
		if (!tile) return gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR);
		gl.generateMipmap(gl.TEXTURE_2D);
		gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR_MIPMAP_LINEAR);
		if (aniso) gl.texParameterf(gl.TEXTURE_2D, aniso.TEXTURE_MAX_ANISOTROPY_EXT, Math.min(16, gl.getParameter(aniso.MAX_TEXTURE_MAX_ANISOTROPY_EXT)));
	});
	['uPlate', 'uMasks', 'uWarm', 'uAurora', 'uIceN', 'uIceR'].forEach((name, unit) => gl.uniform1i(U(name), unit));
	gl.uniform1f(U('uAspect'), SCENE.aspect);
	gl.uniform1f(U('uRiverTop'), SCENE.riverTop);
	gl.uniform1f(U('uSkyLow'), SCENE.skyLow);
	gl.uniform1f(U('uStretch'), SCENE.stretch);
	gl.uniform1f(U('uIglooMirror'), SCENE.iglooMirror);
	gl.uniform1f(U('uHorizon'), SCENE.horizon);

	// Resolution: the device's, up to 2x, lowered while frames take too long.
	let scale = Math.min(window.devicePixelRatio || 1, 2);
	let onScreen = true;
	let running = false;
	let t = SCENE.start;
	let last = 0;
	let slow = 0;
	let frames = 0;
	const draw = () => {
		const w = Math.max(1, Math.round(canvas.clientWidth * scale));
		const h = Math.max(1, Math.round(canvas.clientHeight * scale));
		if (canvas.width !== w || canvas.height !== h) {
			canvas.width = w;
			canvas.height = h;
			gl.viewport(0, 0, w, h);
		}
		// The crop follows the poster's: its object-position (--focus) as a centre in the art's coordinates.
		const visible = Math.min(1, w / h / SCENE.aspect);
		const p = parseFloat(getComputedStyle(art).getPropertyValue('--focus')) / 100 || 0.5;
		gl.uniform2f(U('uFocus'), p * (1 - visible) + visible / 2, 0.5);
		gl.uniform2f(U('uRes'), w, h);
		gl.uniform1f(U('uTime'), t);
		gl.drawArrays(gl.TRIANGLES, 0, 3);
	};
	const frame = (now) => {
		if (!running) return;
		const dt = last ? Math.min((now - last) / 1000, 0.1) : 0;
		last = now;
		t += dt;
		if (frames++ > 10 && dt > 0.028 && ++slow > 20 && scale > 0.75) {
			scale = Math.max(0.75, scale * 0.8);
			slow = 0;
		}
		draw();
		// The canvas replaces the poster once a few frames are on screen, not while the first is compiling.
		if (frames === 3) art.classList.add('is-live');
		requestAnimationFrame(frame);
	};
	const update = () => {
		const run = onScreen && !document.hidden;
		if (run === running) return;
		running = run;
		last = 0;
		if (run) requestAnimationFrame(frame);
	};
	new IntersectionObserver(([entry]) => {
		onScreen = entry.isIntersecting;
		update();
	}).observe(art);
	document.addEventListener('visibilitychange', update);
	canvas.addEventListener('webglcontextlost', (event) => {
		event.preventDefault();
		running = false;
		art.classList.remove('is-live');
	});

	update();
}
