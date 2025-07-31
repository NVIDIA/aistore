/**
 * Copy functionality for curl command examples
 */

function copyCode(button) {
  // Find the next code block after the button
  let current = button.parentElement;
  while (current = current.nextElementSibling) {
    const code = current.querySelector('code');
    if (code && code.textContent.trim()) {
      // Copy to clipboard
      navigator.clipboard.writeText(code.textContent.trim()).then(() => {
        showCopySuccess(button);
      }).catch(err => {
        console.error('Copy failed:', err);
      });
      return;
    }
  }
  console.log('No code block found');
}

function showCopySuccess(button) {
  const originalText = button.textContent;
  button.textContent = 'Copied!';
  button.style.background = '#28a745';
  
  setTimeout(() => {
    button.textContent = originalText;
    button.style.background = '#0366d6';
  }, 2000);
} 