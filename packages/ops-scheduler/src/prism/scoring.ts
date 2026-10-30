export type Workload='standard_transfers'|'room_transfers'|'streaming'|'messages';
export type Pool='qualifying'|'qualified';
export const QUALIFICATION_TASK_TARGETS={standard_transfers:120,room_transfers:40} as const;
export function workloadConfidence(workload:keyof typeof QUALIFICATION_TASK_TARGETS,verifiedOriginalTasks:number,
  successRate:number,ageHours:number):number {
  const verifiedRatio=Math.max(0,Math.min(1,verifiedOriginalTasks/QUALIFICATION_TASK_TARGETS[workload]));
  const maturity=0.8+0.2*Math.max(0,Math.min(1,ageHours/24));
  return verifiedRatio*Math.max(0,Math.min(1,successRate))*maturity;
}
export interface AssignmentResult {successfulTasks:number;accountableTasks:number;durationMs:number|null}
export type DuelResult='upper_win'|'lower_win'|'draw'|'unrated';
export const SPEED_DRAW_PERCENT=5;
export const ZERO_SIGNIFICANCE=3;
export interface WaveParticipant {id:string;assignedTasks:number;finalScore:number;points:string}
function units(value:string):bigint {
  if(!/^-?\d+(\.\d{1,6})?$/.test(value)) throw new Error('Invalid points');
  const negative=value.startsWith('-');const [whole,fraction='']=(negative?value.slice(1):value).split('.');
  return (negative?-1n:1n)*(BigInt(whole!)*1000000n+BigInt(fraction.padEnd(6,'0')));
}
export function orderWave(participants:readonly WaveParticipant[],random:()=>number=Math.random):WaveParticipant[] {
  const result=[...participants];
  for(let i=result.length-1;i>0;i--){const j=Math.floor(random()*(i+1));[result[i],result[j]]=[result[j]!,result[i]!];}
  return result.sort((a,b)=>b.assignedTasks-a.assignedTasks||b.finalScore-a.finalScore
    ||(units(b.points)>units(a.points)?1:units(b.points)<units(a.points)?-1:0));
}
export function duelResult(upper:AssignmentResult,lower:AssignmentResult):DuelResult {
  if(!upper.accountableTasks||!lower.accountableTasks)return 'unrated';
  const a=BigInt(upper.successfulTasks)*BigInt(lower.accountableTasks),b=BigInt(lower.successfulTasks)*BigInt(upper.accountableTasks);
  if(a!==b)return a>b?'upper_win':'lower_win';
  if(!upper.successfulTasks&&!lower.successfulTasks)return 'draw';
  if(upper.durationMs===null||lower.durationMs===null||!Number.isFinite(upper.durationMs)||!Number.isFinite(lower.durationMs))return 'unrated';
  const faster=Math.min(upper.durationMs,lower.durationMs),slower=Math.max(upper.durationMs,lower.durationMs);
  if(slower*100<=faster*(100+SPEED_DRAW_PERCENT))return 'draw';
  return upper.durationMs<lower.durationMs?'upper_win':'lower_win';
}
export function stake(n:number,fleet:number):string {
  if(n<2)return '0';
  if(!Number.isInteger(n)||!Number.isInteger(fleet)||fleet<n)throw new Error('Invalid fleet');
  return String(5+(fleet===2?10:Math.round(10*(n-2)/(fleet-2))));
}
export function duelStake(waveStake:string,upper:AssignmentResult,lower:AssignmentResult):string {
  const base=Number(waveStake);
  if(!base||!upper.accountableTasks||!lower.accountableTasks||!upper.successfulTasks)return waveStake;
  if(BigInt(upper.successfulTasks)*BigInt(lower.accountableTasks)!==BigInt(lower.successfulTasks)*BigInt(upper.accountableTasks))return waveStake;
  if(upper.durationMs===null||lower.durationMs===null||!Number.isFinite(upper.durationMs)||!Number.isFinite(lower.durationMs))return waveStake;
  const faster=Math.min(upper.durationMs,lower.durationMs),slower=Math.max(upper.durationMs,lower.durationMs);
  return String(Math.round(base*(faster>0?Math.min(8,1+Math.log2(slower/faster)):8)));
}
export const RECOVERY_FAILURE_STAKES=3;
export const UNCHARGED_RECOVERY_REASONS=['no_delivery_cut','tail_cut'] as const;
export function recoveryCharge(firstWaveStake:string,chargedRecoveries:number):string {
  return String(-RECOVERY_FAILURE_STAKES*Number(firstWaveStake)*chargedRecoveries);
}
export function transferWideFailure(outcomes:readonly AssignmentResult[]):boolean {
  const rated=outcomes.filter(o=>o.accountableTasks>0);
  return rated.length>=2&&rated.filter(o=>o.successfulTasks<o.accountableTasks).length*2>rated.length;
}
export function scaleBottom(lowest:string,lowestVariance:string):bigint {
  if(!/^\d+$/.test(lowestVariance))throw new Error('Invalid variance');
  const lo=units(lowest),threshold=-BigInt(Math.ceil(ZERO_SIGNIFICANCE*Math.sqrt(Number(lowestVariance))*1000000));
  return lo<0n&&lo<=threshold?lo:threshold<lo?threshold:lo-1000000n;
}
export function normalizePoints(points:string,lowest:string,highest:string,lowestVariance:string):number {
  const p=units(points),lo=units(lowest),hi=units(highest);
  if(lo===hi)return 0.5;
  const bottom=scaleBottom(lowest,lowestVariance);
  return Math.max(0,Math.min(1,Number(p-bottom)/Number(hi-bottom)));
}
export function reliability(promoted:boolean,reliableTasks:number,accountableTasks:number):number {
  if(promoted||!(accountableTasks>0))return 1;
  return Math.max(0,Math.min(1,reliableTasks/accountableTasks));
}
export function verifiedGoodput(bytes:number,durationMs:number|null):number|null {
  return durationMs!==null&&Number.isFinite(durationMs)&&durationMs>0&&Number.isFinite(bytes)&&bytes>=0?bytes*8/durationMs/1000:null;
}
