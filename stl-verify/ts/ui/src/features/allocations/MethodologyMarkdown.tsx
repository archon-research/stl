import ReactMarkdown from 'react-markdown';

type MethodologyMarkdownProps = {
  markdown: string;
};

/**
 * Its own module so `React.lazy` has something to split on: react-markdown pulls
 * ~479 KiB of unified/remark/micromark that nothing else in the app needs.
 */
export default function MethodologyMarkdown({
  markdown,
}: MethodologyMarkdownProps) {
  return <ReactMarkdown>{markdown}</ReactMarkdown>;
}
