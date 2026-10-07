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
  return upper.durationMs===lower.durationMs?'draw':upper.durationMs<lower.durationMs?'upper_win':'lower_win';
}
export function stakes(n:number,fleet:number):{defence:string;upset:string} {
  if(n<2)return {defence:'0.000000',upset:'0.000000'};
  if(!Number.isInteger(n)||!Number.isInteger(fleet)||fleet<n)throw new Error('Invalid fleet');
  const r=fleet===2?1:Math.max(0,Math.min(1,(n-2)/(fleet-2)));
  return {defence:(1+4*r).toFixed(6),upset:(5+10*r).toFixed(6)};
}
export function normalizePoints(points:string,lowest:string,highest:string):number {
  const p=units(points),lo=units(lowest),hi=units(highest);
  return lo===hi?0.5:Math.max(0.2,Math.min(1,0.2+0.8*Number(p-lo)/Number(hi-lo)));
}
export function verifiedGoodput(bytes:number,durationMs:number|null):number|null {
  return durationMs!==null&&Number.isFinite(durationMs)&&durationMs>0&&Number.isFinite(bytes)&&bytes>=0?bytes*8/durationMs/1000:null;
}
