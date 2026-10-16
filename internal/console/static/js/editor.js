// The SQL editor: a plain textarea, so typing, undo, selection and
// accessibility behave natively, over a highlighted copy of its text, with
// line numbers alongside.

import { h } from './dom.js';
import { tokenize } from './sql.js';

// Highlighting a very long script on every keystroke is not worth it.
const maxHighlight = 200_000;

export function createEditor({ value = '', onChange = () => {}, onRun = () => {}, onEscape = () => {}, label = 'SQL' } = {}) {
  const gutter = h('pre', { class: 'gutter', 'aria-hidden': 'true' });
  const code = h('code');
  const highlight = h('pre', { class: 'highlight', 'aria-hidden': 'true' }, code);
  const textarea = h('textarea', {
    spellcheck: 'false', autocapitalize: 'off', autocomplete: 'off', wrap: 'off',
    'aria-label': label, 'aria-keyshortcuts': 'Control+Enter Meta+Enter',
  });
  const el = h('div', { class: 'editor' }, gutter, h('div', { class: 'code' }, highlight, textarea));

  let lines = 0;
  function render() {
    const text = textarea.value;
    const n = text.split('\n').length;
    if (n !== lines) {
      lines = n;
      gutter.textContent = Array.from({ length: n }, (_, i) => i + 1).join('\n') + '\n';
    }
    const frag = document.createDocumentFragment();
    if (text.length <= maxHighlight) {
      for (const [type, part] of tokenize(text)) {
        if (type) frag.append(h('span', { class: `tok-${type}`, text: part }));
        else frag.append(part);
      }
    } else {
      frag.append(text);
    }
    // The trailing newline gives an empty last line its height.
    frag.append('\n');
    code.replaceChildren(frag);
    sync();
  }

  function sync() {
    highlight.scrollTop = textarea.scrollTop;
    highlight.scrollLeft = textarea.scrollLeft;
    gutter.scrollTop = textarea.scrollTop;
  }

  // insert replaces the selection the way typing does, keeping undo.
  function insert(text) {
    if (!document.execCommand('insertText', false, text)) {
      textarea.setRangeText(text, textarea.selectionStart, textarea.selectionEnd, 'end');
      textarea.dispatchEvent(new Event('input'));
    }
  }

  textarea.addEventListener('input', () => { render(); onChange(textarea.value); });
  textarea.addEventListener('scroll', sync);
  textarea.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
      e.preventDefault();
      onRun();
    } else if (e.key === 'Escape') {
      onEscape();
    } else if (e.key === 'Tab' && !e.ctrlKey && !e.metaKey && !e.altKey) {
      e.preventDefault();
      const { selectionStart: start, selectionEnd: end, value: text } = textarea;
      if (start === end && !e.shiftKey) {
        insert('  ');
        return;
      }
      // Indent or outdent the selected lines.
      const from = text.lastIndexOf('\n', start - 1) + 1;
      const block = text.slice(from, end);
      const changed = e.shiftKey
        ? block.replace(/^ {1,2}/gm, '')
        : block.replace(/^/gm, '  ');
      textarea.setSelectionRange(from, end);
      insert(changed);
      textarea.setSelectionRange(from, from + changed.length);
    } else if (e.key === 'Enter' && !e.shiftKey && !e.altKey) {
      // Keep the current line's indentation.
      const { selectionStart: start, value: text } = textarea;
      const lineStart = text.lastIndexOf('\n', start - 1) + 1;
      const indent = /^[ \t]*/.exec(text.slice(lineStart, start))[0];
      if (indent) {
        e.preventDefault();
        insert('\n' + indent);
      }
    }
  });

  textarea.value = value;
  render();

  return {
    el,
    textarea,
    get value() { return textarea.value; },
    set value(v) { textarea.value = v; render(); },
    // selection returns the selected text and where it starts, or the
    // whole text when nothing is selected.
    selection() {
      const { selectionStart: start, selectionEnd: end, value: text } = textarea;
      if (start !== end && text.slice(start, end).trim()) return { text: text.slice(start, end), offset: start };
      return { text, offset: 0 };
    },
    focus() { textarea.focus(); },
    // goTo puts the cursor at a character offset and scrolls to it.
    goTo(offset) {
      textarea.focus();
      textarea.setSelectionRange(offset, offset);
      const line = textarea.value.slice(0, offset).split('\n').length - 1;
      const lineHeight = 20;
      const top = line * lineHeight;
      if (top < textarea.scrollTop || top > textarea.scrollTop + textarea.clientHeight - 2 * lineHeight) {
        textarea.scrollTop = Math.max(0, top - textarea.clientHeight / 3);
      }
      sync();
    },
  };
}
