// The pod and volume of a workspace: one session's checkout, toolchain and virtual display in a kubernetes environment.
// Pod Security `restricted` with nothing added, no credentials and no network (craft.build/network=none). The source
// arrives over exec stdin; the helper `ws` (deploy/container/ws) is copied from the AutoCraft image by an init container.
// The labels are not the build tasks': the build launcher's orphan sweep deletes every autocraft-build pod it does not own.
import { POD_UID, type KubeSettings } from "./pods.ts";
import { WS_IMAGE_PATH, WS_POD_PATH } from "./workspace.ts";

export const WORKSPACE_APP = "autocraft-workspace";
/** The AutoCraft service's pods, which no workspace pod may share a node with. */
const SERVICE_SELECTOR = { namespace: "craft", labels: { "app.kubernetes.io/name": "autocraft" } };
export const WS_CONTAINER = "workspace";
const COPY_CONTAINER = "ws-helper";
export const DEFAULT_WORKSPACE_DEADLINE_SEC = 4 * 3600;

export type WorkspaceLimits = { deadlineSec: number };

/** The first pod and volume of workspace `id`; a resume names its new ones with `incarnation`, so an old one still shutting down never blocks them. */
export const workspaceName = (id: number, incarnation?: string): string => (incarnation ? `ws-${id}-${incarnation}` : `ws-${id}`);
export const workspaceEnvSelector = (envId: string): string => `app.kubernetes.io/name=${WORKSPACE_APP},craft.build/env=${envId}`;
export const workspaceSelector = (id: number): string => `app.kubernetes.io/name=${WORKSPACE_APP},craft.build/workspace=${id}`;

const labels = (envId: string, id: number) => ({ "app.kubernetes.io/name": WORKSPACE_APP, "craft.build/env": envId, "craft.build/workspace": String(id), "craft.build/network": "none" });

const SECURITY = { allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: { drop: ["ALL"] } };

/** What the session's tools expect: a home and caches on the volume, offline cargo, software Vulkan on display :0. */
export const workspaceEnv = (arch: KubeSettings["arch"]): Record<string, string> => ({
  HOME: "/work/home", CARGO_HOME: "/work/cargo", CARGO_TARGET_DIR: "/work/target", CARGO_NET_OFFLINE: "true",
  DISPLAY: ":0", XDG_RUNTIME_DIR: "/run/user", XDG_CACHE_HOME: "/tmp/cache",
  VK_ICD_FILENAMES: `/usr/share/vulkan/icd.d/lvp_icd.${arch}.json`, WGPU_BACKEND: "vulkan", LP_NUM_THREADS: "4",
});

export function workspacePvcSpec(s: KubeSettings, envId: string, id: number, name = workspaceName(id)) {
  return {
    apiVersion: "v1", kind: "PersistentVolumeClaim",
    metadata: { name, namespace: s.namespace, labels: labels(envId, id) },
    spec: { accessModes: ["ReadWriteOnce"], storageClassName: s.storageClass, resources: { requests: { storage: s.workspaceStorage } } },
  };
}

export type WorkspacePodCtx = { s: KubeSettings; envId: string; id: number; appImage: string; toolchainImage: string; limits: WorkspaceLimits; pod?: string; pvc?: string };

export function workspacePodSpec(c: WorkspacePodCtx) {
  const { s } = c;
  return {
    apiVersion: "v1", kind: "Pod",
    metadata: { name: c.pod ?? workspaceName(c.id), namespace: s.namespace, labels: labels(c.envId, c.id) },
    spec: {
      restartPolicy: "Never", automountServiceAccountToken: false, enableServiceLinks: false,
      activeDeadlineSeconds: Math.max(300, Math.round(c.limits.deadlineSec)), terminationGracePeriodSeconds: 10,
      priorityClassName: "craft-workspace",
      nodeSelector: { "kubernetes.io/arch": s.arch === "aarch64" ? "arm64" : "amd64" },
      ...(s.runtimeClassName ? { runtimeClassName: s.runtimeClassName } : {}),
      securityContext: { runAsNonRoot: true, runAsUser: POD_UID, runAsGroup: POD_UID, fsGroup: POD_UID, seccompProfile: { type: "RuntimeDefault" } },
      // A parked workspace's next pod lands on its volume's node by itself: local-path pins the volume there.
      affinity: {
        podAntiAffinity: {
          requiredDuringSchedulingIgnoredDuringExecution: [{
            labelSelector: { matchLabels: SERVICE_SELECTOR.labels },
            namespaceSelector: { matchLabels: { "kubernetes.io/metadata.name": SERVICE_SELECTOR.namespace } },
            topologyKey: "kubernetes.io/hostname",
          }],
          preferredDuringSchedulingIgnoredDuringExecution: [{
            weight: 50,
            podAffinityTerm: { labelSelector: { matchLabels: { "app.kubernetes.io/name": WORKSPACE_APP } }, topologyKey: "kubernetes.io/hostname" },
          }],
        },
      },
      volumes: [
        { name: "work", persistentVolumeClaim: { claimName: c.pvc ?? workspaceName(c.id) } },
        { name: "ws-bin", emptyDir: { sizeLimit: "16Mi" } },
        { name: "tmp", emptyDir: { sizeLimit: s.workspaceEphemeralLimit } },
        { name: "run-user", emptyDir: { medium: "Memory", sizeLimit: "64Mi" } },
      ],
      initContainers: [{
        name: COPY_CONTAINER, image: c.appImage, imagePullPolicy: "IfNotPresent",
        command: ["cp", WS_IMAGE_PATH, WS_POD_PATH],
        resources: { requests: { cpu: "100m", memory: "64Mi", "ephemeral-storage": "64Mi" }, limits: { cpu: "1", memory: "256Mi", "ephemeral-storage": "256Mi" } },
        securityContext: SECURITY,
        volumeMounts: [{ name: "ws-bin", mountPath: "/opt/ws" }],
      }],
      containers: [{
        name: WS_CONTAINER, image: c.toolchainImage, imagePullPolicy: "IfNotPresent",
        command: [WS_POD_PATH, "init"],
        // /work, not /work/src: the runtime would create a missing working directory as root, and `ws untar` writes there.
        workingDir: "/work",
        env: Object.entries(workspaceEnv(s.arch)).map(([name, value]) => ({ name, value })),
        resources: {
          requests: { cpu: s.workspaceCpuRequest, memory: s.workspaceMemoryRequest, "ephemeral-storage": s.workspaceEphemeralRequest },
          limits: { cpu: s.workspaceCpuLimit, memory: s.workspaceMemoryLimit, "ephemeral-storage": s.workspaceEphemeralLimit },
        },
        securityContext: SECURITY,
        volumeMounts: [
          { name: "work", mountPath: "/work" },
          { name: "ws-bin", mountPath: "/opt/ws", readOnly: true },
          { name: "tmp", mountPath: "/tmp" },
          { name: "run-user", mountPath: "/run/user" },
        ],
      }],
    },
  };
}
