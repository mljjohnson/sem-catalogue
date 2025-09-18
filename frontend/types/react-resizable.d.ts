declare module 'react-resizable' {
  import * as React from 'react';

  export type ResizeHandle = 's' | 'w' | 'e' | 'n' | 'sw' | 'nw' | 'se' | 'ne';

  export interface ResizableProps {
    width: number;
    height: number;
    onResize?: (...args: any[]) => void;
    onResizeStart?: (...args: any[]) => void;
    onResizeStop?: (...args: any[]) => void;
    draggableOpts?: any;
    lockAspectRatio?: boolean;
    resizeHandles?: ResizeHandle[];
    handle?: React.ReactNode;
    minConstraints?: [number, number];
    maxConstraints?: [number, number];
    children?: React.ReactNode;
  }

  export const Resizable: React.FC<ResizableProps>;
  export const ResizableBox: React.FC<any>;
}


