declare module 'node:path' {
  const path: {
    join: (...parts: string[]) => string;
    dirname: (input: string) => string;
  };
  export = path;
}

declare module 'node:fs' {
  export const promises: {
    readFile: (path: string, encoding?: string) => Promise<string>;
    writeFile: (path: string, data: string, encoding?: string) => Promise<void>;
    mkdir: (path: string, options?: { recursive?: boolean }) => Promise<void>;
  };
}

declare const process: { cwd(): string };
