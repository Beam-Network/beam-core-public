import {duelResult,stake,transferWideFailure,normalizePoints,type Pool,type Workload,type AssignmentResult,type WaveParticipant} from '../prism/scoring.js';
export interface FrozenWave {workload:Workload;pool:Pool;eligibleFleetSize:number;participants:readonly WaveParticipant[];settledAt:Date}
export interface Duel {index:number;upper:string;lower:string;upperDelta:string;lowerDelta:string;result:string}
export function settleWave(wave:FrozenWave,outcomes:ReadonlyMap<string,AssignmentResult>):Duel[] {
  if(wave.workload==='streaming'||wave.workload==='messages')return [];
  const s=stake(wave.participants.length,wave.eligibleFleetSize),duels:Duel[]=[];
  const neutral=transferWideFailure(wave.participants.flatMap(p=>outcomes.get(p.id)??[]));
  for(let i=wave.participants.length-2;i>=0;i--){
    const upper=wave.participants[i]!,lower=wave.participants[i+1]!,a=outcomes.get(upper.id),b=outcomes.get(lower.id);
    const result=a&&b&&!neutral?duelResult(a,b):'unrated';
    duels.push({index:i,upper:upper.id,lower:lower.id,result,
      upperDelta:result==='upper_win'?s:result==='lower_win'?'-'+s:'0',
      lowerDelta:result==='lower_win'?s:result==='upper_win'?'-'+s:'0'});
  }return duels;
}
export function workloadFinalScore(points:string,min:string,max:string,minVariance:string,readiness:number,penalty:number):number {
  return normalizePoints(points,min,max,minVariance)*readiness*penalty;
}
export function countsInPerformanceWindow(settledAt:Date,now:Date):boolean {
  return settledAt.getTime()>now.getTime()-86400000&&settledAt.getTime()<=now.getTime();
}
