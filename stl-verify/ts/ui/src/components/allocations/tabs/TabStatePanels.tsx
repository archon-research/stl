import { css } from '#styled-system/css';

type TabSelectionPromptProps = {
  message: string;
};

type TabErrorPanelProps = {
  title: string;
  message: string;
};

export function TabSelectionPrompt({ message }: TabSelectionPromptProps) {
  return (
    <div
      className={css({
        borderRadius: 'md',
        borderStyle: 'solid',
        borderWidth: '1px',
        borderColor: 'border.subtle',
        bg: 'surface.subtle',
        p: '4',
      })}
    >
      <p className={css({ m: 0, fontSize: 'sm', color: 'text.muted' })}>
        {message}
      </p>
    </div>
  );
}

export function TabErrorPanel({ title, message }: TabErrorPanelProps) {
  return (
    <div
      className={css({
        borderRadius: 'md',
        borderStyle: 'solid',
        borderWidth: '1px',
        borderColor: 'border.default',
        bg: 'surface.subtle',
        p: '4',
      })}
    >
      <p
        className={css({
          m: 0,
          fontSize: 'sm',
          fontWeight: 'semibold',
          color: 'text.strong',
        })}
      >
        {title}
      </p>
      <p
        className={css({
          m: 0,
          mt: '1.5',
          fontSize: 'sm',
          color: 'text.muted',
        })}
      >
        {message}
      </p>
    </div>
  );
}
